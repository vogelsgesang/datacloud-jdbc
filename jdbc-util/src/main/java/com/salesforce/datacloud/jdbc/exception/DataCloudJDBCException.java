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
package com.salesforce.datacloud.jdbc.exception;

import java.sql.SQLException;
import lombok.Getter;

@Getter
public class DataCloudJDBCException extends SQLException {
    /**
     * The primary error message. E.g., "No such table: 'my_tab'".
     * Should be displayed front and center to the user.
     */
    private String primaryMessage;

    /**
     * A hint for the customer to help them fix the error. E.g., "Did you mean 'my_table'?"
     * Should be displayed in a secondary position to the user, but only if they can
     * actually modify the queries. If queries are automatically generated, the user will
     * have no way to fix the error, so displaying the hint is not helpful.
     */
    private String customerHint;

    /**
     * Additional details about the error.
     */
    private String customerDetail;

    public DataCloudJDBCException() {
        super();
    }

    public DataCloudJDBCException(String reason) {
        super(reason);
    }

    public DataCloudJDBCException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public DataCloudJDBCException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public DataCloudJDBCException(Throwable cause) {
        super(cause);
    }

    public DataCloudJDBCException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public DataCloudJDBCException(String reason, String SQLState, Throwable cause) {
        super(reason, SQLState, cause);
    }

    public DataCloudJDBCException(String reason, String SQLState, int vendorCode, Throwable cause) {
        super(reason, SQLState, vendorCode, cause);
    }

    public DataCloudJDBCException(
            String combinedMessage, String primaryMessage, String SQLState, String customerHint, String customerDetail, Throwable cause) {
        super(combinedMessage, SQLState, 0, cause);

        this.primaryMessage = primaryMessage;
        this.customerHint = customerHint;
        this.customerDetail = customerDetail;
    }
}
