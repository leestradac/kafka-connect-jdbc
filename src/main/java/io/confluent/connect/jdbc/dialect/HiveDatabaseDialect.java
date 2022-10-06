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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Collection;

/**
 * A {@link HiveDatabaseDialect} for Hive.
 */
public class HiveDatabaseDialect extends GenericDatabaseDialect {

  private final Logger log = LoggerFactory.getLogger(HiveDatabaseDialect.class);

  /**
   * The provider for {@link HiveDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(HiveDatabaseDialect.class.getSimpleName(), "spark","databricks","hive2");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new HiveDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public HiveDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
    log.info("-->HiveDatabaseDialect created");
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIMESTAMP";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "STRING";
      case BYTES:
        return "CHAR(1024)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    log.info("-->start buildUpsertQueryStatement table={} keyColumns={} nonKeyColumns={}",
            table, keyColumns, nonKeyColumns);
    //Hive doesn't support PK, TODO: merge so here how the upsert is handled
    ExpressionBuilder builder = expressionBuilder();
    builder.append("insert into ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") values(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());

    log.info("-->end buildUpsertQueryStatement query={}",builder);
    return builder.toString();
  }

  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    log.info("-->start buildCreateTableStatement table={} fields={}",table, fields);
    ExpressionBuilder builder = expressionBuilder();

    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    this.writeColumnsSpec(builder, fields);
    builder.append(") USING DELTA");

    log.info("-->end buildCreateTableStatement query=", builder);
    return builder.toString();
  }

  @Override
  protected void writeColumnsSpec(
          ExpressionBuilder builder,
          Collection<SinkRecordField> fields
  ) {
    log.info("-->HiveDatabaseDialect writeColumnsSpec start. ExpressionBuilder={}",builder);
    ExpressionBuilder.Transform<SinkRecordField> transform = (b, field) -> {
      b.append(System.lineSeparator());
      this.writeColumnSpec(b, field);
    };
    builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
    log.info("-->HiveDatabaseDialect writeColumnsSpec ends. ExpressionBuilder={}",builder);
  }

  @Override
  protected void writeColumnSpec(
          ExpressionBuilder builder,
          SinkRecordField f
  ) {
    log.info("-->HiveDatabaseDialect writeColumnSpec start. ExpressionBuilder={}",builder);
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    //TODO: for now disable defaultValue, the dialect is different in delta table
    //https://stackoverflow.com/questions/73790572/generated-default-value-in-delta-table
    if (f.defaultValue() != null && false) {
      builder.append(" GENERATED ALWAYS AS ");
      formatColumnValue(
              builder,
              f.schemaName(),
              f.schemaParameters(),
              f.schemaType(),
              f.defaultValue()
      );
    } else if (isColumnOptional(f)) {
      //Delta lake doesn't need null. builder.append(" NULL");
    } else {
      builder.append(" NOT NULL");
    }
    log.info("-->HiveDatabaseDialect writeColumnSpec end. ExpressionBuilder={}",builder);
  }

  /*
  @Override
  protected void formatColumnValue(
          ExpressionBuilder builder,
          String schemaName,
          Map<String, String> schemaParameters,
          Schema.Type type,
          Object value
  ) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          builder.append("(CAST(");
          builder.append(value);
          builder.append(" AS ");
          builder.append(getCastTypeBySchemaName(schemaName));
          builder.append(value);
          builder.append("))");
          return;
        case Date.LOGICAL_NAME:
          builder.append("(CAST(");
          builder.append(value);
          builder.append(" AS ");
          builder.append(getCastTypeBySchemaName(schemaName));
          builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone()));
          return;
        case Time.LOGICAL_NAME:
          builder.append("(CAST(");
          builder.append(value);
          builder.append(" AS ");
          builder.append(getCastTypeBySchemaName(schemaName));
          builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value, timeZone()));
          builder.append("))");
          return;
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          builder.append("(CAST(");
          builder.append(value);
          builder.append(" AS ");
          builder.append(getCastTypeBySchemaName(schemaName));
          builder.appendStringQuoted(
                  DateTimeUtils.formatTimestamp((java.util.Date) value, timeZone())
          );
          builder.append("))");
          return;
        default:
          // fall through to regular types
          break;
      }
    }
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        // no escaping required
        builder.append("(CAST(");
        builder.append(value);
        builder.append(" AS ");
        builder.append(getCastTypeBySchemaType(type));
        builder.append("))");
        break;
      case BOOLEAN:
        builder.append("(CAST(");
        // 1 & 0 for boolean is more portable rather than TRUE/FALSE
        builder.append((Boolean) value ? '1' : '0');
        builder.append(" AS ");
        builder.append(getCastTypeBySchemaType(type));
        builder.append("))");
        break;
      case STRING:
        builder.append("(CAST(");
        builder.appendStringQuoted(value);
        builder.append(" AS ");
        builder.append(getCastTypeBySchemaType(type));
        builder.append("))");
        break;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        builder.append("(CAST(");
        builder.appendBinaryLiteral(bytes);
        builder.append(" AS ");
        builder.append(getCastTypeBySchemaType(type));
        builder.append("))");
        break;
      default:
        throw new ConnectException("Unsupported type for column value: " + type);
    }
  }
  */


  /*
  protected String getCastTypeBySchemaName(String schemaName) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIMESTAMP";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          return schemaName;
      }
    }
    return "";
  }*/


  /*protected String getCastTypeBySchemaType(Schema.Type type) {
    switch (type) {
      case INT8:
        return "TINYINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "TINYINT";
      case STRING:
        return "STRING";
      case BYTES:
        return "CHAR(1024)";
      default:
        return type.getName();
    }
  }*/

}
