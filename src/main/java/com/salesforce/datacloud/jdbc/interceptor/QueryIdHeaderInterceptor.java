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
package com.salesforce.datacloud.jdbc.interceptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@RequiredArgsConstructor
public class QueryIdHeaderInterceptor implements HeaderMutatingClientInterceptor {
    static final String HYPER_QUERY_ID = "x-hyperdb-query-id";

    public static final Metadata.Key<String> HYPER_QUERY_ID_KEY =
            Metadata.Key.of(HYPER_QUERY_ID, ASCII_STRING_MARSHALLER);

    private final String queryId;

    @Override
    public void mutate(final Metadata headers) {
        headers.put(HYPER_QUERY_ID_KEY, queryId);
    }
}
