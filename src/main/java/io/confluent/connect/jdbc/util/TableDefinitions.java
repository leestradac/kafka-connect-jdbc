/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;

/**
 * A simple cache of {@link TableDefinition} keyed.
 */
public class TableDefinitions {

  private static final Logger log = LoggerFactory.getLogger(TableDefinitions.class);

  private final Map<TableId, TableDefinition> cache = new HashMap<>();
  private final DatabaseDialect dialect;

  /**
   * Create an instance that uses the specified database dialect.
   *
   * @param dialect the database dialect; may not be null
   */
  public TableDefinitions(DatabaseDialect dialect) {
    this.dialect = dialect;
  }

  /**
   * Get the {@link TableDefinition} for the given table.
   *
   * @param connection the JDBC connection to use; may not be null
   * @param tableId    the table identifier; may not be null
   * @return the cached {@link TableDefinition}, or null if there is no such table
   * @throws SQLException if there is any problem using the connection
   */
  public TableDefinition get(
      Connection connection,
      final TableId tableId
  ) throws SQLException {
    log.info("-->TableDefinition get start");
    TableDefinition dbTable = cache.get(tableId);
    if (dbTable == null) {
      log.info("-->TableDefinition dbTable is null");
      if (dialect.tableExists(connection, tableId)) {
        log.info("-->TableDefinition table Exists, calling describeTable...");
        dbTable = dialect.describeTable(connection, tableId);
        log.info("-->TableDefinition describeTable dbTable={}",dbTable);
        if (dbTable != null) {
          log.info("-->Setting metadata for table {} to {}", tableId, dbTable);
          cache.put(tableId, dbTable);
        }
      }
    }
    log.info("-->TableDefinition return dbTable={}", dbTable);
    log.info("-->TableDefinition get done");
    return dbTable;
  }

  /**
   * Refresh the cached {@link TableDefinition} for the given table.
   *
   * @param connection the JDBC connection to use; may not be null
   * @param tableId    the table identifier; may not be null
   * @return the refreshed {@link TableDefinition}, or null if there is no such table
   * @throws SQLException if there is any problem using the connection
   */
  public TableDefinition refresh(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    log.info("-->TableDefinition refresh start");
    TableDefinition dbTable = dialect.describeTable(connection, tableId);
    if (dbTable != null) {
      log.info("-->Refreshing metadata for table {} to {}", tableId, dbTable);
      cache.put(dbTable.id(), dbTable);
    } else {
      log.warn("Failed to refresh metadata for table {}", tableId);
    }
    log.info("-->TableDefinition refresh return dbTable={}",dbTable);
    log.info("-->TableDefinition refresh done");
    return dbTable;
  }
}
