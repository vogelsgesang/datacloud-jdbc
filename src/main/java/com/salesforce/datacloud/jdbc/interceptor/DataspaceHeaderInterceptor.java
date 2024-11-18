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

import io.grpc.Metadata;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DataspaceHeaderInterceptor implements HeaderMutatingClientInterceptor {
    public static DataspaceHeaderInterceptor of(Properties properties) {
        return optional(properties, DATASPACE)
                .map(DataspaceHeaderInterceptor::new)
                .orElse(null);
    }

    @NonNull private final String dataspace;

    static final String DATASPACE = "dataspace";

    private static final Metadata.Key<String> DATASPACE_KEY = Metadata.Key.of(DATASPACE, ASCII_STRING_MARSHALLER);

    @Override
    public void mutate(final Metadata headers) {
        headers.put(DATASPACE_KEY, dataspace);
    }

    @Override
    public String toString() {
        return ("DataspaceHeaderInterceptor(dataspace=" + dataspace + ")");
    }
}
