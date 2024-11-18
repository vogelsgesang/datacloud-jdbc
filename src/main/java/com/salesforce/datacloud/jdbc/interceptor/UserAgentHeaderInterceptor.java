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

import static com.salesforce.datacloud.jdbc.util.PropertiesExtensions.optional;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.salesforce.datacloud.jdbc.config.DriverVersion;
import io.grpc.Metadata;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@Slf4j
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class UserAgentHeaderInterceptor implements HeaderMutatingClientInterceptor {
    public static UserAgentHeaderInterceptor of(Properties properties) {
        val provided = optional(properties, USER_AGENT);
        val userAgent = getCombinedUserAgent(provided.orElse(null));
        return new UserAgentHeaderInterceptor(userAgent);
    }

    private final String userAgent;

    @Override
    public void mutate(final Metadata headers) {
        headers.put(USER_AGENT_KEY, userAgent);
    }

    private static final String USER_AGENT = "User-Agent";

    private static final Metadata.Key<String> USER_AGENT_KEY = Metadata.Key.of(USER_AGENT, ASCII_STRING_MARSHALLER);

    private static String getCombinedUserAgent(String clientProvidedUserAgent) {
        String driverInfo = DriverVersion.formatDriverInfo();
        if (clientProvidedUserAgent == null || clientProvidedUserAgent.isEmpty()) {
            return driverInfo;
        }

        if (clientProvidedUserAgent.equals(driverInfo)) {
            return driverInfo;
        }

        return String.format("%s %s", clientProvidedUserAgent, driverInfo);
    }
}
