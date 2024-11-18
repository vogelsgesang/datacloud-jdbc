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
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import io.grpc.Metadata;
import java.util.Properties;
import java.util.UUID;
import lombok.val;
import org.junit.jupiter.api.Test;

class UserAgentHeaderInterceptorTest {
    @Test
    void ofReturnsBasicWithNoUserAgent() {
        val metadata = new Metadata();
        sut(null).mutate(metadata);

        assertThat(metadata.get(Metadata.Key.of("User-Agent", ASCII_STRING_MARSHALLER)))
                .isEqualTo(DriverVersion.formatDriverInfo());
    }

    @Test
    void noDuplicateDriverInfo() {
        val metadata = new Metadata();
        sut(DriverVersion.formatDriverInfo()).mutate(metadata);

        assertThat(metadata.get(Metadata.Key.of("User-Agent", ASCII_STRING_MARSHALLER)))
                .isEqualTo(DriverVersion.formatDriverInfo());
    }

    @Test
    void appliesDataspaceValueToMetadata() {
        val expected = UUID.randomUUID().toString();

        val metadata = new Metadata();
        sut(expected).mutate(metadata);

        assertThat(metadata.get(Metadata.Key.of("User-Agent", ASCII_STRING_MARSHALLER)))
                .isEqualTo(expected + " " + DriverVersion.formatDriverInfo());
    }

    private static UserAgentHeaderInterceptor sut(String userAgent) {
        val properties = new Properties();

        if (userAgent != null) {
            properties.put("User-Agent", userAgent);
        }

        return UserAgentHeaderInterceptor.of(properties);
    }
}
