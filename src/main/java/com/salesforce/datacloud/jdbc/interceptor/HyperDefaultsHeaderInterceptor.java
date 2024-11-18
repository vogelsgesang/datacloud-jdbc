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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
public class HyperDefaultsHeaderInterceptor implements HeaderMutatingClientInterceptor {
    private static final String GRPC_MAX_METADATA_SIZE = String.valueOf(1024 * 1024); // 1mb
    private static final String WORKLOAD_VALUE = "jdbcv3";
    private static final String WORKLOAD_KEY_STR = "x-hyperdb-workload";
    private static final String MAX_METADATA_SIZE = "grpc.max_metadata_size";

    private static final Metadata.Key<String> WORKLOAD_KEY = Metadata.Key.of(WORKLOAD_KEY_STR, ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SIZE_KEY = Metadata.Key.of(MAX_METADATA_SIZE, ASCII_STRING_MARSHALLER);

    @Override
    public void mutate(final Metadata headers) {
        headers.put(WORKLOAD_KEY, WORKLOAD_VALUE);
        headers.put(SIZE_KEY, GRPC_MAX_METADATA_SIZE);
    }
}
