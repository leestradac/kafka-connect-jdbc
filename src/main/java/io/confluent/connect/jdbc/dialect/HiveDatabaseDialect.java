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
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
  public Map<ColumnId, ColumnDefinition> describeColumns(
          Connection connection,
          String catalogPattern,
          String schemaPattern,
          String tablePattern,
          String columnPattern
  ) throws SQLException {
    log.info("-->describeColumns HIVE start");
    log.info("-->describeColumns HIVE connection={}",connection);
    log.info("-->describeColumns HIVE connection.getTypeInfo={}",
            connection.getMetaData().getTypeInfo());
    log.info("-->describeColumns HIVE connection.getURL={}",
            connection.getMetaData().getURL());
    log.info("-->describeColumns HIVE connection.getIdentifierQuoteString={}",
            connection.getMetaData().getIdentifierQuoteString());
    log.info("-->describeColumns HIVE connection.getClientInfoProperties={}",
            connection.getMetaData().getClientInfoProperties());
    log.info("-->describeColumns HIVE catalogPattern={}",catalogPattern);
    log.info("-->describeColumns HIVE schemaPattern={}",schemaPattern);
    log.info("-->describeColumns HIVE tablePattern={}",tablePattern);
    log.info("-->describeColumns HIVE columnPattern={}",columnPattern);
    log.info("-->Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
            this,
            catalogPattern,
            schemaPattern,
            tablePattern
    );

    // Get the primary keys of the table(s) ...
    log.info("-->describeColumns HIVE calling primaryKeyColumns");
    final Set<ColumnId> pkColumns = primaryKeyColumns(
            connection,
            catalogPattern,
            schemaPattern,
            tablePattern
    );
    Map<ColumnId, ColumnDefinition> results = new HashMap<>();
    log.info("-->describeColumns HIVE connection.getMetaData().getColumns");
    try (ResultSet rs = connection.getMetaData().getColumns(
            catalogPattern,
            schemaPattern,
            tablePattern,
            columnPattern
    )) {
      log.info("-->describeColumns HIVE connection.getMetaData().getColumns success");

      extracted(pkColumns, results, rs);

      log.info("-->describeColumns HIVE results.size={}",
              results.size());
      log.info("-->describeColumns HIVE end");
      return results;
    }
  }

  private void extracted(Set<ColumnId> pkColumns, Map<ColumnId, ColumnDefinition> results,
                         ResultSet rs) throws SQLException {


    final int rsColumnCount = rs.getMetaData().getColumnCount();
    log.info("--->rsColumnCount={}",rsColumnCount);

    int i = 0;
    log.info("-->before while loop rs={}",rs);
    while (rs.next() || i < rsColumnCount) {
      log.info("--->describeColumns HIVE reading rs. {}/{}",++i, rsColumnCount);

      final String catalogName = rs.getMetaData().getCatalogName(i);//rs.getString(1);
      final String schemaName = rs.getMetaData().getSchemaName(i);//rs.getString(2);
      final String tableName = rs.getMetaData().getTableName(i);//rs.getString(3);
      log.info("--->describeColumns HIVE catalogName={} schemaName={} tableName={}",
              catalogName,schemaName,tableName);
      final TableId tableId = new TableId(catalogName, schemaName, tableName);
      final String columnName = rs.getMetaData().getColumnName(i);//rs.getString(4);
      final ColumnId columnId = new ColumnId(tableId, columnName, null);
      final int jdbcType = rs.getMetaData().getColumnType(i);//rs.getInt(5); ??
      final String typeName = rs.getMetaData().getColumnTypeName(i);//rs.getString(6); ??
      log.info("--->describeColumns HIVE columnName={} columnId={} jdbcType={} typeName={}",
              columnName,columnId,jdbcType,typeName);
      final int precision = rs.getMetaData().getPrecision(i);//rs.getInt(7);
      final int scale = rs.getMetaData().getScale(i);//rs.getInt(9);
      final String typeClassName = null;
      ColumnDefinition.Nullability nullability;
      final int nullableValue = rs.getMetaData().isNullable(i);//rs.getInt(11);
      log.info("--->describeColumns HIVE nullableValue={}",nullableValue);
      switch (nullableValue) {
        case DatabaseMetaData.columnNoNulls:
          nullability = ColumnDefinition.Nullability.NOT_NULL;
          break;
        case DatabaseMetaData.columnNullable:
          nullability = ColumnDefinition.Nullability.NULL;
          break;
        case DatabaseMetaData.columnNullableUnknown:
        default:
          nullability = ColumnDefinition.Nullability.UNKNOWN;
          break;
      }
      Boolean autoIncremented = null;

      if (rsColumnCount >= 23) {
        // Not all drivers include all columns ...
        log.info("-->rsColumnCount>=23. Not all drivers include all columns");
        autoIncremented = rs.getMetaData().isAutoIncrement(i);
        log.info("-->rsColumnCount>=23. autoIncremented={}", autoIncremented);
      }
      Boolean signed = null;
      Boolean caseSensitive = null;
      Boolean searchable = null;
      Boolean currency = null;
      Integer displaySize = null;
      boolean isPrimaryKey = pkColumns.contains(columnId);
      if (isPrimaryKey) {
        // Some DBMSes report pks as null
        nullability = ColumnDefinition.Nullability.NOT_NULL;
      }
      ColumnDefinition defn = columnDefinition(
              rs,
              columnId,
              jdbcType,
              typeName,
              typeClassName,
              nullability,
              ColumnDefinition.Mutability.UNKNOWN,
              precision,
              scale,
              signed,
              displaySize,
              autoIncremented,
              caseSensitive,
              searchable,
              currency,
              isPrimaryKey
      );
      log.info("--->describeColumns HIVE results add columnId={}, defn={}",
              columnId,
              defn);

      results.put(columnId, defn);
    }
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

  public String buildInsertStatementOrg(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(this.columnValueVariables(definition))
            .of(keyColumns, nonKeyColumns);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildInsertStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {
    String result = buildUpsertQueryStatement(table, keyColumns, nonKeyColumns);
    log.info("-->buildInsertStatement {}",result);
    return result;
  }

  @Override
  public String buildUpdateStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns,
          TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(this.columnNamesWithValueVariables(definition))
            .of(nonKeyColumns);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
              .delimitedBy(" AND ")
              .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
              .of(keyColumns);
    }
    return builder.toString();
  }

  @Override
  public String buildUpsertQueryStatement(
          final TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    log.info("-->buildUpsertQueryStatement HIVE");
    // https://db.apache.org/derby/docs/10.11/ref/rrefsqljmerge.html
    final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
      builder.append(table)
              .append(".")
              .appendColumnName(col.name())
              .append("=DAT.")
              .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" using (values(");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.placeholderInsteadOfColumnNames("?"))
            .of(keyColumns, nonKeyColumns);
    builder.append(") as DAT(");
    builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
    builder.append(")) on ");
    builder.appendList()
            .delimitedBy(" and ")
            .transformedBy(transform)
            .of(keyColumns);

    log.info("-->buildUpsertQueryStatement HIVE builder={}",builder);

    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      log.info("-->buildUpsertQueryStatement HIVE ...when matched then update set...");
      builder.append(" when matched then update set ");
      builder.appendList()
              .delimitedBy(", ")
              .transformedBy(transform)
              .of(nonKeyColumns);
    }

    log.info("-->buildUpsertQueryStatement HIVE builder+KeyColumns={}",builder);

    builder.append(" when not matched then insert(");
    builder.appendList().delimitedBy(",").of(nonKeyColumns, keyColumns);
    builder.append(") values(");
    builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNamesWithPrefix("DAT."))
            .of(nonKeyColumns, keyColumns);
    builder.append(")");

    log.info("-->buildUpsertQueryStatement HIVE DONE FINAL builder={}",builder);
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


  /**
   * Return the transform that produces an assignment expression each with the name of one of the
   * columns and the prepared statement variable. PostgreSQL may require the variable to have a
   * type suffix, such as {@code ?::uuid}.
   *
   * @param d the table definition; may be null if unknown
   * @return the transform that produces the assignment expression for use within a prepared
   *         statement; never null
   */
  protected ExpressionBuilder.Transform<ColumnId> columnNamesWithValueVariables(TableDefinition d) {
    return (builder, columnId) -> {
      builder.appendColumnName(columnId.name());
      builder.append(" = ?");
      builder.append(valueTypeCast(d, columnId));
    };
  }


  /**
   * Return the transform that produces a prepared statement variable for each of the columns.
   * PostgreSQL may require the variable to have a type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the variable expression for each column; never null
   */
  protected ExpressionBuilder.Transform<ColumnId> columnValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.append("?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the typecast expression that can be used as a suffix for a value variable of the
   * given column in the defined table.
   *
   * <p>This method returns a blank string except for those column types that require casting
   * when set with literal values. For example, a column of type {@code uuid} must be cast when
   * being bound with with a {@code varchar} literal, since a UUID value cannot be bound directly.
   *
   * @param tableDefn the table definition; may be null if unknown
   * @param columnId  the column within the table; may not be null
   * @return the cast expression, or an empty string; never null
   */
  protected String valueTypeCast(TableDefinition tableDefn, ColumnId columnId) {
    if (tableDefn != null) {
      ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
      if (defn != null) {
        String typeName = defn.typeName(); // database-specific
        if (typeName != null) {
          typeName = typeName.toLowerCase();
          //TODO Luis:??? we don't have cast types in delta?
          //if (CAST_TYPES.contains(typeName)) {return "::" + typeName;}
        }
      }
    }
    return "";
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
