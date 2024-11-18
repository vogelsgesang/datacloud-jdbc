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
package com.salesforce.datacloud.jdbc.core;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class HyperConnectionSettings {
    private static final String HYPER_SETTING = "serverSetting.";
    private final Map<String, String> settings;

    public static HyperConnectionSettings of(Properties properties) {
        val result = properties.entrySet().stream()
                .filter(e -> e.getKey().toString().startsWith(HYPER_SETTING))
                .collect(
                        Collectors.toMap(e -> e.getKey().toString().substring(HYPER_SETTING.length()), e -> e.getValue()
                                .toString()));
        return new HyperConnectionSettings(result);
    }

    public Map<String, String> getSettings() {
        return Collections.unmodifiableMap(settings);
    }
}
