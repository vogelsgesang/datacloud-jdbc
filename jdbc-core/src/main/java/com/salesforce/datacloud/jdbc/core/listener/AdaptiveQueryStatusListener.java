/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.core.listener;

import com.salesforce.datacloud.jdbc.core.DataCloudResultSet;
import com.salesforce.datacloud.jdbc.core.HyperGrpcClientExecutor;
import com.salesforce.datacloud.jdbc.core.StreamingResultSet;
import com.salesforce.datacloud.jdbc.core.partial.ChunkBased;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import com.salesforce.datacloud.jdbc.exception.QueryExceptionHandler;
import com.salesforce.datacloud.jdbc.util.StreamUtilities;
import com.salesforce.datacloud.query.v3.DataCloudQueryStatus;
import io.grpc.StatusRuntimeException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import salesforce.cdp.hyperdb.v1.ExecuteQueryResponse;
import salesforce.cdp.hyperdb.v1.QueryResult;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Deprecated
public class AdaptiveQueryStatusListener implements QueryStatusListener {
    private static final String BEFORE_READY = "The remaining adaptive results were requested before ready";

    @Getter
    private final String queryId;

    private final HyperGrpcClientExecutor client;

    private final Duration timeout;

    private final Iterator<ExecuteQueryResponse> response;

    private final AtomicReference<DataCloudQueryStatus> lastStatus = new AtomicReference<>();

    public static AdaptiveQueryStatusListener of(String query, HyperGrpcClientExecutor client, Duration timeout)
            throws SQLException {
        try {
            val response = client.executeQuery(query);
            val queryId = response.next().getQueryInfo().getQueryStatus().getQueryId();

            log.info("Executing adaptive query. queryId={}, timeout={}", queryId, timeout);

            return new AdaptiveQueryStatusListener(queryId, client, timeout, response);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(query, ex);
        }
    }

    public static RowBasedAdaptiveQueryStatusListener of(
            String query, HyperGrpcClientExecutor client, Duration timeout, long maxRows) throws SQLException {
        try {
            val response = client.executeQuery(query, maxRows);
            val queryId = response.next().getQueryInfo().getQueryStatus().getQueryId();

            log.info("Executing adaptive query. queryId={}, timeout={}", queryId, timeout);

            return new RowBasedAdaptiveQueryStatusListener(queryId, client, response);
        } catch (StatusRuntimeException ex) {
            throw QueryExceptionHandler.createQueryException(query, ex);
        }
    }

    @Override
    public DataCloudResultSet generateResultSet() throws DataCloudJDBCException {
        return StreamingResultSet.of(queryId, client, stream().iterator());
    }

    @Override
    public Stream<QueryResult> stream() throws DataCloudJDBCException {
        return Stream.<Supplier<Stream<QueryResult>>>of(this::head, this::tail).flatMap(Supplier::get);
    }

    private Stream<QueryResult> head() {
        return StreamUtilities.toStream(response)
                .map(this::mapHead)
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    private Optional<QueryResult> mapHead(ExecuteQueryResponse item) {
        Optional.ofNullable(item)
                .map(ExecuteQueryResponse::getQueryInfo)
                .flatMap(DataCloudQueryStatus::of)
                .ifPresent(lastStatus::set);
        return Optional.ofNullable(item).map(ExecuteQueryResponse::getQueryResult);
    }

    private boolean allResultsInHead() {
        return Optional.ofNullable(lastStatus.get())
                .map(s -> s.allResultsProduced() && s.getChunkCount() < 2)
                .orElse(false);
    }

    @SneakyThrows
    private Stream<QueryResult> tail() {
        if (allResultsInHead()) {
            return Stream.empty();
        }

        val status = client.waitForResultsProduced(queryId, timeout);

        if (!status.allResultsProduced()) {
            throw new DataCloudJDBCException(BEFORE_READY + ". queryId=" + queryId + ", timeout=" + timeout);
        }

        if (status.getChunkCount() < 2) {
            return Stream.empty();
        }

        val iterator = ChunkBased.of(client, queryId, 1, status.getChunkCount() - 1, true);
        return StreamUtilities.toStream(iterator);
    }

    @Slf4j
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    @Deprecated
    public static class RowBasedAdaptiveQueryStatusListener implements QueryStatusListener {
        @Getter
        private final String queryId;

        private final HyperGrpcClientExecutor client;

        private final Iterator<ExecuteQueryResponse> response;

        private final AtomicReference<DataCloudQueryStatus> lastStatus = new AtomicReference<>();

        @Override
        public DataCloudResultSet generateResultSet() throws DataCloudJDBCException {
            return StreamingResultSet.of(queryId, client, stream().iterator());
        }

        @Override
        public Stream<QueryResult> stream() throws DataCloudJDBCException {
            return StreamUtilities.toStream(response)
                    .map(this::mapHead)
                    .filter(Optional::isPresent)
                    .map(Optional::get);
        }

        private Optional<QueryResult> mapHead(ExecuteQueryResponse item) {
            Optional.ofNullable(item)
                    .map(ExecuteQueryResponse::getQueryInfo)
                    .flatMap(DataCloudQueryStatus::of)
                    .ifPresent(lastStatus::set);
            return Optional.ofNullable(item).map(ExecuteQueryResponse::getQueryResult);
        }
    }
}
