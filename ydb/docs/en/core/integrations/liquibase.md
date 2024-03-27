# Support for the YDB dialect in the Liquibase migration tool. 

## Introduction {#introductuin}

In today's world of software development, database schema versioning is a critical task to ensure consistency and the ability to roll back changes. Database migration tools, such as Liquibase, provide tools for versioning, tracking, and applying changes to the schema. 

However, the emergence of new database management systems (DBMS) has made it necessary to integrate with these systems in order to support their unique capabilities and SQL dialects. 

We will discuss the results of integrating DB dialect with Liquibase in this article.

## Features of the {{ ydb-short-name }} Dialect {#ydb-dialect}

Liquibase's main functionality is the abstract description of database schemas in `.xml`, `.json`, or `.yaml` format. This ensures portability when switching between different database management systems (DBMS).

The following basic features are supported in the {{ ydb-short-name }} dialect:

1. `createTable`: table creation. The types described in the SQL standards are compared to the primitive types in YDB. For example, bigint will be converted to Int64, and you can specify the original table name (such as Int32 or Json). However, portability is lost in this case.

2. `dropTable`, `alterTable`, `createIndex`, `addColumn`: deleting tables, changing them, creating indexes, and adding columns.

3. `loadData`, `loadUpdateData`: uploading data from CSV files.

To learn more about which SQL features YDB supports, please refer to the [YQL](https://ydb.tech/docs/en/yql/reference/) documentation. It is important to note that YQL custom queries can be executed through native SQL. 

## How to use it? {#using}

There are two ways: programmatically from a Java / Kotlin application or via the Liquibase CLI. Using Java / Kotlin is described in detail in the [README](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect) of the project; there's also a link to an example Spring Boot app.

### Example of using a dialect {#example}

First, you need to install the Liquibase utility itself using [existing methods](https://docs.liquibase.com/start/install/home.html). Then, you need to place the actual .jar archives for the [JDBC driver](https://github.com/ydb-platform/ydb-jdbc-driver) and YDB [dialect](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect) for Liquibase.

```bash
cp ydb-jdbc-driver-shaded-2.0.7.jar ./liquibase/internal/lib/
cp liquibase-ydb-dialect-0.9.6.jar ./liquibase/internal/lib/
```

For a more detailed description, please see the [Manual library management](https://docs.liquibase.com/start/install/home.html). Now the liquibase utility can be used in standard ways.

Let's write a simple `liquibase.properties`:

```properties
changelog-file=changelogs.xml
url=jdbc:ydb:grpc://localhost:2136/local
```

Now, let's move on to migrating the data schema.

Let's create the first table and add two records there:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <changeSet id="series" author="kurdyukov-kir">
        <comment>Table series.</comment>

        <createTable tableName="series">
            <column name="series_id" type="Int64">
                <constraints primaryKey="true"/>
            </column>

            <column name="title" type="text"/>
            <column name="series_info" type="text"/>
            <column name="release_date" type="date"/>
        </createTable>

        <createIndex tableName="series" indexName="series_index" unique="false">
            <column name="title"/>
        </createIndex>

        <rollback>
            <dropTable tableName="series"/>
            <dropIndex tableName="series" indexName="series_index"/>
        </rollback>
    </changeSet>

    <changeSet id="added_data_into_series" author="kurdyukov-kir">
        <insert tableName="series">
            <column name="series_id" valueNumeric="1"/>
            <column name="title" value="IT Crowd"/>
            <column name="series_info"
                    value="The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."/>
            <column name="release_date" valueDate="2006-02-03"/>
        </insert>
        <insert tableName="series">
            <column name="series_id" valueNumeric="2"/>
            <column name="title" value="Silicon Valley"/>
            <column name="series_info"
                    value="Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."/>
            <column name="release_date" valueDate="2014-04-06"/>
        </insert>
    </changeSet>
</databaseChangeLog>
```

Content changelogs.xml:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <include file="/migration/series.xml" relativeToChangelogFile="true"/>
</databaseChangeLog>
```

After executing the `liquibase update` command, Liquibase will print a log of the executed migrations:

```bash
i113855673:liquibase kurdyukov-kir$ liquibase update
march. 07, 2024 6:42:34 PM tech.ydb.jdbc.YdbDriver register
INFO: YDB JDBC Driver registered: tech.ydb.jdbc.YdbDriver@4b45dcb8
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
 ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ###
##   _     _             _ _                      ##
##  | |   (_)           (_) |                     ##
##  | |    _  __ _ _   _ _| |__   __ _ ___  ___   ##
##  | |   | |/ _` | | | | | '_ \ / _` / __|/ _ \  ##
##  | |___| | (_| | |_| | | |_) | (_| \__ \  __/  ##
##  \_____/_|\__, |\__,_|_|_.__/ \__,_|___/\___|  ##
##              | |                               ##
##              |_|                               ##
##                                                ## 
##  Get documentation at docs.liquibase.com       ##
##  Get certified courses at learn.liquibase.com  ## 
##                                                ##
 ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ###
Starting Liquibase at 18:42:35 (version 4.25.1 #690 built at 2023-12-18 16:29+0000)
Liquibase Version: 4.25.1
Liquibase Open Source 4.25.1 by Liquibase
Running Changeset: migration/series.xml::series::kurdyukov-kir
Running Changeset: migration/series.xml::added_data_into_series::kurdyukov-kir

UPDATE SUMMARY
Run:                          2
Previously run:               0
Filtered out:                 0
-------------------------------
Total change sets:            2

Liquibase: Update has been successful. Rows affected: 4
Liquibase command 'update' was executed successfully.
```

Next, you can see two system tables created by Liquibase in your database: DATABASECHANGELOG, DATABASECHANGELOGLOCK.

Records DATABASECHANGELOG:

| AUTHOR | COMMENTS | CONTEXTS | DATEEXECUTED | DEPLOYMENT\_ID | DESCRIPTION | EXECTYPE | FILENAME | ID | LABELS | LIQUIBASE | MD5SUM | ORDEREXECUTED | TAG |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| kurdyukov-kir |  | null | 15:42:40 | 9826159656 | insert tableName=series; insert tableName=series | EXECUTED | migration/series.xml | added\_data\_into\_series | null | 4.25.1 | 9:cb49879b530528bc2555422bb7db58da | 2 | null |
| kurdyukov-kir | Table series. | null | 15:42:40 | 9826159656 | createTable tableName=series; createIndex indexName=series\_index, tableName=series | EXECUTED | migration/series.xml | series | null | 4.25.1 | 9:5809802102bcd74f1d8bc0f1d874463f | 1 | null |


Next, let's assume that our database schema requires additional migrations.

Let's create the seasons and episodes tables:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <changeSet id="seasons" author="kurdyukov-kir">
        <comment>Table seasons.</comment>

        <createTable tableName="seasons">
            <column name="series_id" type="bigint">
                <constraints primaryKey="true"/>
            </column>
            <column name="season_id" type="bigint">
                <constraints primaryKey="true"/>
            </column>

            <column name="title" type="text"/>
            <column name="first_aired" type="datetime"/>
            <column name="last_aired" type="datetime"/>
        </createTable>

        <insert tableName="seasons">
            <column name="series_id" valueNumeric="1"/>
            <column name="season_id" valueNumeric="1"/>
            <column name="title" value="Season 1"/>
            <column name="first_aired" valueDate="2019-09-16T10:00:00"/>
            <column name="last_aired" valueDate="2023-09-16T12:30:00"/>
        </insert>
        <rollback>
            <dropTable tableName="seasons"/>
        </rollback>
    </changeSet>

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
    </changeSet>
</databaseChangeLog>
```

Upload the data from the CSV file to the "episodes" table:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <changeSet id="episodes-from-csv" author="kurdyukov-kir" context="all">
        <loadData tableName="episodes" file="./csv/episodes.csv" relativeToChangelogFile="true"/>
    </changeSet>
</databaseChangeLog>
```

Create YDB topic:

```sql
--liquibase formatted sql

--changeset kurdyukov-kir:10
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer
    ) WITH (retention_period = Interval('P1D')
);
```

Content changelogs.xml:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                      http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">

    <include file="/migration/series.xml" relativeToChangelogFile="true"/>
    <include file="/migration/seasons_and_episodes.xml" relativeToChangelogFile="true"/>
    <include file="/migration/load_episodes_data.xml" relativeToChangelogFile="true"/>
    <include file="/migration/sql/topic.sql" relativeToChangelogFile="true"/>
</databaseChangeLog>
```

After executing `liquibase update`, the database schema will be successfully updated.

![YDB UI after apply migrations](../_assets/liquibase-result-example.png =450x)
