# Migrating {{ ydb-short-name }} data schemas with the Liquibase migration tool

## Introduction {#introduction}

[Liquibase](https://www.liquibase.com/) is an open-source library for tracking, managing, and applying changes to database schemas. It is extended with dialects for different database management systems (DBMS), including {{ ydb-short-name }}.

A dialect is the main entity in the Liquibase framework that helps generate SQL queries for a database, taking into account the specifics of a particular DBMS.

## Features of the {{ ydb-short-name }} dialect {#ydb-dialect}

Liquibase's main functionality is the abstract description of a database schema in `.xml`, `.json`, or `.yaml` formats. This ensures portability when switching from one DBMS to another.

The dialect supports the basic constructs of the standard migration description (changeset).

### Creating a table

The `createTable` changeset is responsible for creating a table. Type descriptions from the SQL standard are mapped to primitive {{ ydb-short-name }} types. For example, the `bigint` type will be converted to `Int64`.

{% note info %}

You can also explicitly specify the original type name, such as `Int32`, `Json`, `JsonDocument`, `Bytes`, or `Interval`. However, in this case, the schema won't be portable.

{% endnote %}

Table of default Liquibase type mappings to [{{ ydb-short-name }} types](../../yql/reference/types/primitive.md):

| Liquibase types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | {{ ydb-short-name }} type  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `boolean`, `java.sql.Types.BOOLEAN`, `java.lang.Boolean`, `bit`, `bool`                                                                                                                                                                                                                                                                                                                                                                                                                                       | `Bool`                     |
| `blob`, `longblob`, `longvarbinary`, `String`, `java.sql.Types.BLOB`, `java.sql.Types.LONGBLOB`, `java.sql.Types.LONGVARBINARY`, `java.sql.Types.VARBINARY`,`java.sql.Types.BINARY`, `varbinary`, `binary`, `image`, `tinyblob`, `mediumblob`, `long binary`, `long varbinary`                                                                                                                                                                                                                                | `Bytes` (synonym `String`) |
| `java.sql.Types.DATE`, `smalldatetime`, `date`                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `Date`                     |
| `decimal`, `java.sql.Types.DECIMAL`, `java.math.BigDecimal`                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `Decimal(22,9)`            |
| `double`, `java.sql.Types.DOUBLE`, `java.lang.Double`                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `Double`                   |
| `float`, `java.sql.Types.FLOAT`, `java.lang.Float`, `real`, `java.sql.Types.REAL`                                                                                                                                                                                                                                                                                                                                                                                                                             | `Float`                    |
| `int`, `integer`, `java.sql.Types.INTEGER`, `java.lang.Integer`, `int4`, `int32`                                                                                                                                                                                                                                                                                                                                                                                                                              | `Int32`                    |
| `bigint`, `java.sql.Types.BIGINT`, `java.math.BigInteger`, `java.lang.Long`, `integer8`, `bigserial`, `long`                                                                                                                                                                                                                                                                                                                                                                                                  | `Int64`                    |
| `java.sql.Types.SMALLINT`, `int2`, `smallserial`, `smallint`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `Int16`                    |
| `java.sql.Types.TINYINT`, `tinyint`                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `Int8`                     |
| `char`, `java.sql.Types.CHAR`, `bpchar`, `character`, `nchar`, `java.sql.Types.NCHAR`, `nchar2`, `text`, `varchar`, `java.sql.Types.VARCHAR`, `java.lang.String`, `varchar2`, `character varying`, `nvarchar`, `java.sql.Types.NVARCHAR`, `nvarchar2`, `national`, `clob`, `longvarchar`, `longtext`, `java.sql.Types.LONGVARCHAR`, `java.sql.Types.CLOB`, `nclob`, `longnvarchar`, `ntext`, `java.sql.Types.LONGNVARCHAR`, `java.sql.Types.NCLOB`, `tinytext`, `mediumtext`, `long varchar`, `long nvarchar` | `Text` (synonym `Utf8`)    |
| `timestamp`, `java.sql.Types.TIMESTAMP`, `java.sql.Timestamp`                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `Timestamp`                |
| `datetime`, `time`, `java.sql.Types.TIME`, `java.sql.Time`                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `Datetime`                 |

{% note warning %}

In {{ ydb-short-name }}, the `Timestamp` data type stores dates with microsecond precision, while `java.sql.Timestamp` or `java.time.Instant` store timestamps with nanosecond precision. Be aware of this when working with this data type.

Type names are case insensitive.

{% endnote %}

`dropTable` - deletes a table. Example: `<dropTable tableName="episodes"/>`

### Extended date and interval types {#extended-datetime}

To create columns with an extended date or interval range, specify the native type names `Date32`, `Datetime64`, `Timestamp64`, and `Interval64` in the `type` attribute. The standard names `date`, `datetime`, and `timestamp` still create `Date`, `Datetime`, and `Timestamp`; extended types are not selected automatically.

For example, add the following changeset to the migration changelog:

```xml
<changeSet id="extended-datetime" author="example">
    <createTable tableName="events">
        <column name="id" type="Int64">
            <constraints primaryKey="true" nullable="false"/>
        </column>
        <column name="event_date" type="Date32"/>
        <column name="event_datetime" type="Datetime64"/>
        <column name="event_timestamp" type="Timestamp64"/>
        <column name="event_interval" type="Interval64"/>
    </createTable>
</changeSet>
```

The same type names can be used in `addColumn`, as well as in YAML and JSON migration changelogs. Value ranges and precision are described in the [Date and time](../../yql/reference/types/primitive.md#datetime) section.

Use the [JDBC driver](../../reference/languages-and-apis/jdbc-driver/maven.md) version 2.3.11 or newer: this version adds support for extended types. Specifying a column type does not add automatic conversion of string values for `insert`, `loadData`, and `loadUpdateData`. To load extended dates and intervals, use SQL migrations with explicitly typed values.

### Decimal with specified precision and scale {#decimal}

In {{ ydb-short-name }}, the `Decimal(precision, scale)` type allows you to specify the total number of decimal digits and the number of digits after the decimal point. Valid values are `1 ≤ precision ≤ 35` and `0 ≤ scale ≤ precision`; see [Primitive data types](../../yql/reference/types/primitive.md#numeric) for details.

{% note info %}

The following examples require `liquibase-ydb-dialect` version [1.1.3](https://github.com/ydb-platform/ydb-java-dialects/blob/main/liquibase-dialect/CHANGELOG.md) or newer. In earlier releases, the `decimal` handler uses the fixed type `Decimal(22,9)`, including when other parameters are explicitly specified.

{% endnote %}

To create a column with the desired precision and scale, specify both parameters in the `type` attribute:

```xml
<changeSet id="parameterized-decimal" author="example">
    <createTable tableName="decimal_values">
        <column name="id" type="Int64">
            <constraints primaryKey="true" nullable="false"/>
        </column>
        <column name="amount" type="Decimal(35,10)"/>
    </createTable>
</changeSet>
```

The dialect preserves precision and scale in `createTable`, `addColumn`, when writing via `insert`, `loadData`, and `loadUpdateData`, and when generating a migration changelog from an existing schema. No SQL changeset is required for an arbitrary `Decimal(precision, scale)`. The same parameters are supported in YAML and JSON migration changelogs and for the names `java.sql.Types.DECIMAL` and `java.math.BigDecimal`.

If parameters are not specified, for example `type="decimal"`, `Decimal(22,9)` is used. A single parameter specifies precision with zero scale: `type="decimal(10)"` creates `Decimal(10,0)`. More than two parameters, non-integer values, or values outside the valid range result in a type validation error.

When used with Hibernate, specify the same precision and scale [in the entity class](../orm/hibernate.md#decimal).

### Changing the table structure

`addColumn` - adds a column. Example:

{% list tabs group=format %}

- xml

  ```xml
  <addColumn tableName="seasons">
      <column name="is_deleted" type="bool"/>
  </addColumn>
  ```

- json

  ```json
  "changes": [
      {
        "addColumn": {
          "tableName": "seasons",
          "columns": [
            {
              "column": {
                "name": "is_deleted",
                "type": "bool"
              }
            }
          ]
        }
      }
    ]
  ```

- yaml

  ```yaml
  changes:
  - addColumn:
      tableName: seasons
      columns:
      - column:
          name: is_deleted
          type: bool
  ```

{% endlist %}

`createIndex` - creates a secondary index. Example:

{% list tabs group=format %}

- xml

  ```xml
  <createIndex tableName="episodes" indexName="episodes_index">
      <column name="title"/>
  </createIndex>
  ```

- json

  ```json
  "changes": [
      {
        "createIndex": {
          "tableName": "episodes",
          "indexName": "episodes_index",
          "columns": {
            "column": {
              "name": "title"
            }
          }
        }
      }
  ```

- yaml

  ```yaml
  changes:
  - createIndex:
      tableName: episodes
      indexName: episodes_index
      columns:
      - column:
          name: title
  ```

{% endlist %}

{% note warning %}

{{ ydb-short-name }} doesn't support unique secondary indexes.

{% endnote %}

{% note info %}

Asynchronous indexes should be created using [native SQL migrations](https://docs.liquibase.com/concepts/changelogs/sql-format.html).

{% endnote %}

`dropIndex` - drops a secondary index. Example:

{% list tabs group=format %}

- xml

  ```xml
  <dropIndex tableName="series" indexName="series_index"/>
  ```

- json

  ```json
  "changes": [
    {
      "dropIndex": {
        "tableName": "series",
        "indexName": "series_index"
      }
    }
  ]
  ```

- yaml

  ```yaml
  changes:
  - dropIndex:
      tableName: series
      indexName: series_index
  ```

{% endlist %}

### Ingesting data into a table

`loadData`, `loadUpdateData` - load data from a `CSV` file. `loadUpdateData` loads data using the [{#T}](../../yql/reference/syntax/upsert_into.md) command.

`insert` - a changeset that performs a single insert into a table using the [INSERT INTO](../../yql/reference/syntax/insert_into.md) command. For example:

{% list tabs group=format %}

- xml

  ```xml
  <insert tableName="episodes">
      <column name="series_id" valueNumeric="1"/>
      <column name="season_id" valueNumeric="1"/>
      <column name="episode_id" valueNumeric="1"/>
      <column name="title" value="Yesterday's Jam"/>
      <column name="air_date" valueDate="2023-04-03T08:46:23.456"/>
  </insert>
  ```

- json

  ```json
  "changes": [
    {
      "insert": {
        "tableName": "episodes",
        "columns": [
          {
            "column": {
              "name": "series_id",
              "valueNumeric": "1"
            }
          },
          {
            "column": {
              "name": "season_id",
              "valueNumeric": "1"
            }
          },
          {
            "column": {
              "name": "episode_id",
              "valueNumeric": "1"
            }
          },
          {
            "column": {
              "name": "title",
              "value": "Yesterday's Jam"
            }
          },
          {
            "column": {
              "name": "air_date",
              "valueDate": "2023-04-03T08:46:23.456"
            }
          }
        ]
      }
    }
  ]
  ```

- yaml

  ```yaml
  changes:
  - insert:
      tableName: episodes
      columns:
        - column:
            name: series_id
            valueNumeric: 1
        - column:
            name: season_id
            valueNumeric: 1
        - column:
            name: episode_id
            valueNumeric: 1
        - column:
            name: title
            value: Yesterday's Jam
        - column:
            name: air_date
            valueDate: 2023-04-03T08:46:23.456
  ```

{% endlist %}

For the types in the table below, values in the `value` field of the `insert` changeset or in CSV files are automatically converted to the corresponding {{ ydb-short-name }} types. For [extended dates and intervals](#extended-datetime), use SQL migrations. Passing values of a [parameterized Decimal](#decimal) preserves the column's precision and scale.

Table of type formatting for loading into a table:

| {{ ydb-short-name }} type                                 | Description format                                                                                                                                                                                                                                           |
|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Bool`                                  | `true` or `false`                                                                                                                                                                                                                                        |
| `Int8`, `Int16`, `Int32`, `Int64`       | A signed integer                                                                                                                                                                                                                                       |
| `Uint8`, `Uint16`, `Uint32`, `Uint64`   | An unsigned integer                                                                                                                                                                                                                                     |
| `Text`, `Bytes`, `Json`, `JsonDocument` | Represent as text                                                                                                                                                                                                                                        |
| `Float`, `Double`, `Decimal(precision, scale)`     | A real number                                                                                                                                                                                                                                        |
| `Interval`                              | [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601#Durations), corresponds to the `java.time.Duration` class in Java.                                                                                                                                           |
| `Date`                                  | Pattern `YYYY-MM-DD` calendar date from the standard [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)                                                                                                                                                      |
| `Datetime`                              | Pattern `YYYY-MM-DDThh:mm:ss`, timezone will be set to `UTC`                                                                                                                                                                                            |
| `Timestamp`                             | Timestamp from the [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) standard corresponds to the `java.time.Instant` class in Java, timezone will be set to `UTC` (precision in microseconds - a limitation of the `Timestamp` data type in {{ ydb-short-name }}) |

Example `CSV` file:

```csv
id,bool,bigint,smallint,tinyint,float,double,decimal,uint8,uint16,uint32,uint64,text,binary,json,jsondocument,date,datetime,timestamp,interval
2,true,123123,13000,112,1.123,1.123123,1.123123,12,13,14,15,kurdyukov-kir,binary,{"asd": "asd"},{"asd": "asd"},2014-04-06,2023-09-16T12:30,2023-07-31T17:00:00.00Z,PT10S
```

{% note warning %}

To understand which SQL constructs {{ ydb-short-name }} can execute and what data type restrictions exist, refer to the [YQL](../../yql/reference/index.md) query language documentation.

{% endnote %}

{% note tip %}

It is important to note that custom YQL instructions can be applied using [native SQL queries](https://docs.liquibase.com/concepts/changelogs/sql-format.html).

{% endnote %}

## How to use it? {#using}

There are two ways:

{% list tabs %}

- Programmatically from Java / Kotlin applications

  How to use it from Java / Kotlin is described in detail in the project's [README](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect). There is also an [example Spring Boot application](https://github.com/ydb-platform/ydb-java-examples/tree/master/jdbc/spring-liquibase-app) that uses it.

- Liquibase CLI

  First, you need to install the liquibase utility itself using [any of the recommended methods](https://docs.liquibase.com/start/install/home.html). Then you need to place the current `.jar` archives of the [{{ ydb-short-name }} JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases) and the Liquibase [{{ ydb-short-name }} dialect](https://mvnrepository.com/artifact/tech.ydb.dialects/liquibase-ydb-dialect/1.1.3) into the `internal/lib` folder.

  ```bash
  # $(which liquibase)
  cd ./internal/lib/

  # you may need to sudo
  # set actual versions of .jar files
  curl -L -o ydb-jdbc-driver.jar https://repo.maven.apache.org/maven2/tech/ydb/jdbc/ydb-jdbc-driver-shaded/2.4.1/ydb-jdbc-driver-shaded-2.4.1.jar
  curl -L -o liquibase-ydb-dialect.jar https://repo.maven.apache.org/maven2/tech/ydb/dialects/liquibase-ydb-dialect/1.1.3/liquibase-ydb-dialect-1.1.3.jar
  ```

  A more detailed description is in the [manual library management](https://docs.liquibase.com/start/install/home.html) section of the Liquibase documentation.

  Now the liquibase command line utility can be used with {{ ydb-short-name }}.

{% endlist %}

## Liquibase usage scenarios

### Initializing liquibase on an empty {{ ydb-short-name }}

The main command is `liquibase update`, which applies migrations if the current {{ ydb-short-name }} schema lags behind the user-defined description.

The examples below assume the following `liquibase.properties` configuration file:

```properties
changelog-file=changelogs.xml
url=jdbc:ydb:grpc://localhost:2136/local
```

Let's apply the following changeset to an empty database:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <changeSet id="episodes" author="kurdyukov-kir">
        <comment>Table episodes.</comment>

        <createTable tableName="episodes">
            <column name="series_id" type="bigint">
                <constraints primaryKey="true"/>
            </column>
            <column name="season_id" type="bigint">
                <constraints primaryKey="true"/>
            </column>
            <column name="episode_id" type="bigint">
                <constraints primaryKey="true"/>
            </column>

            <column name="title" type="text"/>
            <column name="air_date" type="timestamp"/>
        </createTable>
        <rollback>
            <dropTable tableName="episodes"/>
        </rollback>
    </changeSet>
    <changeSet id="index_episodes_title" author="kurdyukov-kir">
        <createIndex tableName="episodes" indexName="index_episodes_title">
            <column name="title"/>
        </createIndex>
    </changeSet>
</databaseChangeLog>
```

After executing the `liquibase update` command, liquibase will print the following log:

```bash
UPDATE SUMMARY
Run:                          2
Previously run:               0
Filtered out:                 0
-------------------------------
Total change sets:            2

Liquibase: Update has been successful. Rows affected: 2
Liquibase command 'update' was executed successfully.
```

After applying the migrations, the data schema looks as follows:

![_assets/liquibase-step-1.png](_assets/liquibase-step-1.png)

You can see that Liquibase has created two service tables: `DATABASECHANGELOG` - the migration log, and `DATABASECHANGELOGLOCK` - a table for acquiring a distributed lock.

Contents of the DATABASECHANGELOG table:

| AUTHOR        | COMMENTS        | CONTEXTS | DATEEXECUTED | DEPLOYMENT_ID | DESCRIPTION                                                    | EXECTYPE | FILENAME               | ID                   | LABELS | LIQUIBASE | MD5SUM                             | ORDEREXECUTED | TAG |
|:--------------|:----------------|:---------|:-------------|:--------------|:---------------------------------------------------------------|:---------|:-----------------------|:---------------------|:-------|:----------|:-----------------------------------|:--------------|:----|
| kurdyukov-kir | Table episodes. |          | 12:53:27     | 1544007500    | createTable tableName=episodes                                 | EXECUTED | migration/episodes.xml | episodes             |        | 4.25.1    | 9:4067056a5ab61db09b379a93625870ca | 1             | |
| kurdyukov-kir | ""              |          | 12:53:28     | 1544007500    | createIndex indexName=index_episodes_title, tableName=episodes | EXECUTED | migration/episodes.xml | index_episodes_title |        | 4.25.1    | 9:49b8b0b22d18c7fd90a3d6b2c561455d | 2             | |

### Database schema evolution

Let's say we need to create a {{ ydb-short-name }} topic and disable the `AUTO_PARTITIONING_BY_SIZE` parameter of the table. This can be done with a native SQL script:

```yql
--liquibase formatted sql

--changeset kurdyukov-kir:create-a-topic
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer
) WITH (
     retention_period = Interval('P1D')
);

--changeset kurdyukov-kir:auto-partitioning-disabled
ALTER TABLE episodes SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

Let's also add a new column `is_deleted` and drop the `index_episodes_title` index:

```xml
<changeSet id="alter-episodes" author="kurdyukov-kir">
    <comment>Alter table episodes.</comment>

    <dropIndex tableName="episodes" indexName="index_episodes_title"/>

    <addColumn tableName="episodes">
        <column name="is_deleted" type="bool"/>
    </addColumn>
</changeSet>
<include file="/migration/sql/yql.sql" relativeToChangelogFile="true"/>
```

After executing `liquibase update`, the database schema will be successfully updated.

```text
UPDATE SUMMARY
Run:                          3
Previously run:               2
Filtered out:                 0
-------------------------------
Total change sets:            5

Liquibase: Update has been successful. Rows affected: 3
Liquibase command 'update' was executed successfully.
```

The result will be dropping the index, adding the `is_deleted` column, disabling the `AUTO_PARTITIONING_BY_SIZE` parameter, and creating the topic:

![_assets/liquibase-step-2.png](_assets/liquibase-step-2.png)

### Initializing liquibase in a project with a non-empty data schema

Let's suppose there is an existing project with the current database schema:

![_assets/liquibase-step-3.png](_assets/liquibase-step-3.png)

To start using liquibase, you need to run:

```bash
liquibase generate-changelog --changelog-file=changelog.xml
```

Contents of the generated changelog.xml:

```xml
<changeSet author="kurdyukov-kir (generated)" id="1711556283305-1">
    <createTable tableName="all_types_table">
        <column name="id" type="INT32">
            <constraints nullable="false" primaryKey="true"/>
        </column>
        <column name="bool_column" type="BOOL"/>
        <column name="bigint_column" type="INT64"/>
        <column name="smallint_column" type="INT16"/>
        <column name="tinyint_column" type="INT8"/>
        <column name="float_column" type="FLOAT"/>
        <column name="double_column" type="DOUBLE"/>
        <column name="decimal_column" type="DECIMAL(22, 9)"/>
        <column name="uint8_column" type="UINT8"/>
        <column name="uint16_column" type="UINT16"/>
        <column name="unit32_column" type="UINT32"/>
        <column name="unit64_column" type="UINT64"/>
        <column name="text_column" type="TEXT"/>
        <column name="binary_column" type="BYTES"/>
        <column name="json_column" type="JSON"/>
        <column name="jsondocument_column" type="JSONDOCUMENT"/>
        <column name="date_column" type="DATE"/>
        <column name="datetime_column" type="DATETIME"/>
        <column name="timestamp_column" type="TIMESTAMP"/>
        <column name="interval_column" type="INTERVAL"/>
    </createTable>
</changeSet>
<changeSet author="kurdyukov-kir (generated)" id="1711556283305-2">
    <createTable tableName="episodes">
        <column name="series_id" type="INT64">
            <constraints nullable="false" primaryKey="true"/>
        </column>
        <column name="season_id" type="INT64">
            <constraints nullable="false" primaryKey="true"/>
        </column>
        <column name="episode_id" type="INT64">
            <constraints nullable="false" primaryKey="true"/>
        </column>
        <column name="title" type="TEXT"/>
        <column name="air_date" type="DATE"/>
    </createTable>
</changeSet>
<changeSet author="kurdyukov-kir (generated)" id="1711556283305-3">
    <createIndex indexName="title_index" tableName="episodes">
        <column name="title"/>
    </createIndex>
</changeSet>
```

Then you need to synchronize the generated changelog.xml file, which is done with the command:

```bash
liquibase changelog-sync --changelog-file=dbchangelog.xml
```

The result will be liquibase synchronization in your project:

![_assets/liquibase-step-4.png](_assets/liquibase-step-4.png)

## Connecting to {{ ydb-short-name }} {#connect-to-ydb}

In the examples above, we used a Docker container, which didn't require additional authentication parameters.

There are various authentication options through the URL parameter:

* Docker container (anonymous authentication):<br/>`jdbc:ydb:grpc://localhost:2136/local`
* Self-hosted cluster:<br/>`jdbc:ydb:grpcs://<host>:2135/Root/testdb?secureConnectionCertificate=file:~/myca.cer`
* Connection using a token:<br/>`jdbc:ydb:grpcs://<host>:2135/path/to/database?token=file:~/my_token`
* Connection using a service account:<br/>`jdbc:ydb:grpcs://<host>:2135/path/to/database?saFile=file:~/sa_key.json`

If your cluster is configured with a username and password, authentication is done through Liquibase parameters.

For more information about various authentication settings, refer to the corresponding [section](../../security/authentication.md).
