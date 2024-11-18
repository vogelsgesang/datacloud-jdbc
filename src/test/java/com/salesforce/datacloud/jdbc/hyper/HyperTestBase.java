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
package com.salesforce.datacloud.jdbc.hyper;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.core.DataCloudConnection;
import com.salesforce.datacloud.jdbc.core.DataCloudStatement;
import com.salesforce.datacloud.jdbc.interceptor.AuthorizationHeaderInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HyperTestBase {
    private static final String LISTENING = "gRPC listening on";

    @SneakyThrows
    public static void assertEachRowIsTheSame(ResultSet rs, AtomicInteger prev) {
        val expected = prev.incrementAndGet();
        val a = rs.getBigDecimal(1).intValue();
        assertThat(expected).isEqualTo(a);
    }

    @SafeVarargs
    @SneakyThrows
    public static void assertWithConnection(
            ThrowingConsumer<DataCloudConnection> assertion, Map.Entry<String, String>... settings) {
        try (val connection = getHyperQueryConnection(settings == null ? Map.of() : Map.ofEntries(settings))) {
            assertion.accept(connection);
        }
    }

    @SafeVarargs
    @SneakyThrows
    public static void assertWithStatement(
            ThrowingConsumer<DataCloudStatement> assertion, Map.Entry<String, String>... settings) {
        try (val connection = getHyperQueryConnection(settings == null ? Map.of() : Map.ofEntries(settings));
                val result = connection.createStatement().unwrap(DataCloudStatement.class)) {
            assertion.accept(result);
        }
    }

    public static DataCloudConnection getHyperQueryConnection() {
        return getHyperQueryConnection(Map.of());
    }

    @SneakyThrows
    public static DataCloudConnection getHyperQueryConnection(Map<String, String> connectionSettings) {

        val properties = new Properties();
        properties.putAll(connectionSettings);
        val auth = AuthorizationHeaderInterceptor.of(new NoopTokenSupplier());
        val channel = ManagedChannelBuilder.forAddress("localhost", 8181).usePlaintext();

        return DataCloudConnection.fromTokenSupplier(auth, channel, properties);
    }

    private static Process hyperProcess;
    private static final ExecutorService hyperMonitors = Executors.newFixedThreadPool(2);

    public static boolean enabled() {
        return hyperProcess != null && hyperProcess.isAlive();
    }

    @AfterAll
    public void afterAll() {
        try {
            if (hyperProcess != null && hyperProcess.isAlive()) {
                hyperProcess.destroy();
            }
        } catch (Throwable e) {
            log.error("Failed to destroy hyperd", e);
        }

        try {
            hyperMonitors.shutdown();
        } catch (Throwable e) {
            log.error("Failed to shutdown hyper monitor thread pool", e);
        }
    }

    @SneakyThrows
    @BeforeAll
    public void beforeAll() {
        if (hyperProcess != null) {
            log.info("hyperd was started but not cleaned up?");
            return;
        } else {
            log.info("starting hyperd, this might take a few seconds");
        }

        val hyperd = new File("./target/hyper/hyperd");
        val properties = Paths.get(requireNonNull(HyperTestBase.class.getResource("/hyper.yaml"))
                        .toURI())
                .toFile();

        if (!hyperd.exists()) {
            Assertions.fail("hyperd executable couldn't be found, have you run mvn process-test-resources? expected="
                    + hyperd.getAbsolutePath());
        }

        hyperProcess = new ProcessBuilder()
                .command(hyperd.getAbsolutePath(), "--config", properties.getAbsolutePath(), "--no-password", "run")
                .start();

        val latch = new CountDownLatch(1);

        hyperMonitors.execute(() -> logStream(hyperProcess.getErrorStream(), log::error));
        hyperMonitors.execute(() -> logStream(hyperProcess.getInputStream(), line -> {
            log.info(line);
            if (line.contains(LISTENING)) {
                latch.countDown();
            }
        }));

        if (!latch.await(30, TimeUnit.SECONDS)) {
            Assertions.fail("failed to start instance of hyper within 30 seconds");
        }
    }

    @BeforeEach
    public void assumeHyperEnabled() {
        Assumptions.assumeTrue(enabled(), "Hyper wasn't started so skipping test");
    }

    static class NoopTokenSupplier implements AuthorizationHeaderInterceptor.TokenSupplier {
        @Override
        public String getToken() {
            return "";
        }
    }

    private static void logStream(InputStream inputStream, Consumer<String> consumer) {
        try (val reader = new BufferedReader(new BufferedReader(new InputStreamReader(inputStream)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                consumer.accept(line);
            }
        } catch (Exception e) {
            log.error("Caught exception while consuming log stream", e);
        }
    }
}
