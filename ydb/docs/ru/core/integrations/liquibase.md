# Миграции схемы данных {{ ydb-short-name }} с помощью Liquibase {#ydb-dialect}

## Введение {#introduction}

[Liquibase](https://www.liquibase.com/) – это библиотека с открытым исходным кодом для отслеживания, управления и применения изменений схемы базы данных. Liquibase расширяется диалектами для различных СУБД.

Диалект - это основная сущность в фреймворке Liquibase, которая помогают формировать SQL запросы к базе данных, учитывая специфику той или иной СУБД.

## Возможности диалекта {{ ydb-short-name }} {#ydb-dialect}

Основной функциональностью Liquibase является абстрактное описание схемы базы данных в `.xml`, `.json` или `.yaml` форматах. Что обеспечивает переносимость при смене одной СУБД на другую.

В диалекте поддержаны основные конструкции стандарта описания миграций (changeset).

### Создание таблицы

Changeset `createTable` отвечает за создание таблицы. Описание типов из SQL стандарта сопоставляется с примитивными типами {{ ydb-short-name }}. К примеру тип bigint будет конвертирован в Int64.

{% note info %}

Также можно явно указывать оригинальное название, например, такие типы как `Int32`, `Json`, `JsonDocument`, `Bytes`, `Interval`. Но в таком случае теряется переносимость схемы.

{% endnote %}

Таблица сравнения описаний типов Liquibase с [типами {{ ydb-short-name }}](https://ydb.tech/docs/ru/yql/reference/types/primitive):

| Liquibase types                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | {{ ydb-short-name }} type  |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `boolean`, `java.sql.Types.BOOLEAN`, `java.lang.Boolean`, `bit`, `bool`                                                                                                                                                                                                                                                                                                                                                                                                                                       | `Bool`                     |
| `blob`, `longblob`, `longvarbinary`, `String`, `java.sql.Types.BLOB`, `java.sql.Types.LONGBLOB`, `java.sql.Types.LONGVARBINARY`, `java.sql.Types.VARBINARY`,`java.sql.Types.BINARY`, `varbinary`, `binary`, `image`, `tinyblob`, `mediumblob`, `long binary`, `long varbinary`                                                                                                                                                                                                                                | `Bytes` (синоним `String`) |
| `java.sql.Types.DATE`, `smalldatetime`, `date`                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `Date`                     |
| `decimal`, `java.sql.Types.DECIMAL`, `java.math.BigDecimal`                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `Decimal(22,9)`            |
| `double`, `java.sql.Types.DOUBLE`, `java.lang.Double`                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `Double`                   |
| `float`, `java.sql.Types.FLOAT`, `java.lang.Float`, `real`, `java.sql.Types.REAL`                                                                                                                                                                                                                                                                                                                                                                                                                             | `Float`                    |
| `int`, `integer`, `java.sql.Types.INTEGER`, `java.lang.Integer`, `int4`, `int32`                                                                                                                                                                                                                                                                                                                                                                                                                              | `Int32`                    |
| `bigint`, `java.sql.Types.BIGINT`, `java.math.BigInteger`, `java.lang.Long`, `integer8`, `bigserial`, `long`                                                                                                                                                                                                                                                                                                                                                                                                  | `Int64`                    |
| `java.sql.Types.SMALLINT`, `int2`, `smallserial`, `smallint`                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `Int16`                    |
| `java.sql.Types.TINYINT`, `tinyint`                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `Int8`                     |
| `char`, `java.sql.Types.CHAR`, `bpchar`, `character`, `nchar`, `java.sql.Types.NCHAR`, `nchar2`, `text`, `varchar`, `java.sql.Types.VARCHAR`, `java.lang.String`, `varchar2`, `character varying`, `nvarchar`, `java.sql.Types.NVARCHAR`, `nvarchar2`, `national`, `clob`, `longvarchar`, `longtext`, `java.sql.Types.LONGVARCHAR`, `java.sql.Types.CLOB`, `nclob`, `longnvarchar`, `ntext`, `java.sql.Types.LONGNVARCHAR`, `java.sql.Types.NCLOB`, `tinytext`, `mediumtext`, `long varchar`, `long nvarchar` | `Text`                     |
| `timestamp`, `java.sql.Types.TIMESTAMP`, `java.sql.TIMESTAMP`                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `Timestamp`                |
| `java.util.Date`, `time`, `java.sql.Types.TIME`, `java.sql.Time`                                                                                                                                                                                                                                                                                                                                                                                                                                              | `Datetime`                 |

{% note info %}

Регистр типа данных не имеет значения.

{% endnote %}

### Изменение структуры таблицы

`dropTable` - удаление таблицы. Пример: `<dropTable tableName="episodes"/>`

`addColumn` - добавление колонки. Пример:

```xml

<addColumn tableName="seasons">
    <column name="is_deleted" type="bool"/>
</addColumn>
```

`createIndex` - создание индекса. Пример:

```xml

<createIndex tableName="episodes" indexName="episodes_index" unique="false">
    <column name="title"/>
</createIndex>
```

{% note info %}

Создание асинхронных индексов нужно делать через нативные SQL миграции.

{% endnote %}

`dropIndex` - удаление индекса. Пример: `<dropIndex tableName="series" indexName="series_index"/>`

### Вставка данных в таблицу

`loadData`, `loadUpdateData` - загрузка данных из CSV файла. *loadUpdate* загружает данные командой UPSERT. Данные будут автоматически приведены к нужным типам, учитывая строгую типизацию {{ ydb-short-name }}.

`insert` - changeset, который осуществляет единичный insert в таблицу. Значение можно объявлять в поле value, например `<column name="timestamp_column" value="2023-07-31T17:00:00.123123Z"/>`.

{% note warning %}

Чтобы понять, какие SQL-конструкции может выполнять {{ ydb-short-name }}, ознакомьтесь с документацией по языку запросов [YQL](https://ydb.tech/docs/ru/yql/reference/).

{% endnote %}

{% note tip %}

Важно отметить, что кастомные инструкции YQL можно применять через нативные SQL запросы.

{% endnote %}

## Как воспользоваться? {#using}

Есть два способа:

{% list tabs %}

- Программно из Java / Kotlin приложения

  Как воспользоваться из Java / Kotlin подробно описано в [README](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect) проекта, там же есть ссылка на пример Spring Boot приложения.

- Liquibase CLI

  Для начала нужно установить саму утилиту liquibase [любым из рекомендуемых способов](https://docs.liquibase.com/start/install/home.html). Затем нужно подложить актуальные .jar архивы [{{ ydb-short-name }} JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases) и Liquibase [диалекта {{ ydb-short-name }}](https://mvnrepository.com/artifact/tech.ydb.dialects/liquibase-ydb-dialect/1.0.0).

  ```bash
  # $(which liquibase)
  cd ./internal/lib/
  
  # you may need to sudo
  # set an actual versions .jar files
  curl -L -o ydb-jdbc-driver.jar https://repo1.maven.org/maven2/tech/ydb/jdbc/ydb-jdbc-driver-shaded/2.0.7/ydb-jdbc-driver-shaded-2.0.7.jar
  curl -L -o liquibase-ydb-dialect.jar https://repo1.maven.org/maven2/tech/ydb/dialects/liquibase-ydb-dialect/1.0.0/liquibase-ydb-dialect-1.0.0.jar
  ```

  Более подробное описание в разделе [Manual library management](https://docs.liquibase.com/start/install/home.html). 
  
  Теперь liquibase утилитой можно пользоваться стандартными способами.

{% endlist %}

## Сценарии использования liquibase

### Инициализация liquibase на пустой {{ ydb-short-name }}

Основной командой является `liquibase update`, которая применяет миграции, если текущая схема {{ ydb-short-name }} отстает от пользовательского описания.

Применим к пустой базе данных следующий changeset:

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
        <createIndex tableName="episodes" indexName="index_episodes_title" unique="false">
            <column name="title"/>
        </createIndex>
    </changeSet>
</databaseChangeLog>
```

После исполнения команды `liquibase update`, liquibase напечатает следующий лог:

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

После применения миграций схемы данных выглядит следующим образом:

![../_assets/liquibase-step-1.png](../_assets/liquibase-step-1.png)

Можно увидеть, что Liquibase создал две служебные таблицы `DATABASECHANGELOG` - лог миграций, `DATABASECHANGELOGLOCK` - таблица для взятия распределенной блокировки.

Содержимое таблицы DATABASECHANGELOG:

| AUTHOR        | COMMENTS        | CONTEXTS | DATEEXECUTED | DEPLOYMENT_ID | DESCRIPTION                                                    | EXECTYPE | FILENAME               | ID                   | LABELS | LIQUIBASE | MD5SUM                             | ORDEREXECUTED | TAG |
|:--------------|:----------------|:---------|:-------------|:--------------|:---------------------------------------------------------------|:---------|:-----------------------|:---------------------|:-------|:----------|:-----------------------------------|:--------------|:----|
| kurdyukov-kir | Table episodes. |          | 12:53:27     | 1544007500    | createTable tableName=episodes                                 | EXECUTED | migration/episodes.xml | episodes             |        | 4.25.1    | 9:4067056a5ab61db09b379a93625870ca | 1             |
| kurdyukov-kir | ""              |          | 12:53:28     | 1544007500    | createIndex indexName=index_episodes_title, tableName=episodes | EXECUTED | migration/episodes.xml | index_episodes_title |        | 4.25.1    | 9:49b8b0b22d18c7fd90a3d6b2c561455d | 2             |

### Эволюция схемы базы данных

Допустим нам нужно создать {{ ydb-short-name }} топик и выключить авто партиционирования таблицы. Это можно сделать нативным SQL скриптом:

```sql
--liquibase formatted sql

--changeset kurdyukov-kir:10
CREATE
TOPIC `my_topic` (
    CONSUMER my_consumer
    ) WITH (retention_period = Interval('P1D')
);

--changeset kurdyukov-kir:auto-partitioning-disabled
ALTER TABLE episodes SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

Также добавим новую колонку `is_deleted` и удалим индекс `index_episodes_title`:

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

После исполнения `liquibase update` схема базы успешно обновится.

```bash
UPDATE SUMMARY
Run:                          3
Previously run:               2
Filtered out:                 0
-------------------------------
Total change sets:            5

Liquibase: Update has been successful. Rows affected: 3
Liquibase command 'update' was executed successfully.
```

Результатом будет удаление индекса, добавление колонки `is_deleted`, выключение настройки авто партиционирования, а также создание топика:

![../_assets/liquibase-step-2.png](../_assets/liquibase-step-2.png)

### Инициализация liquibase в проекте с непустой схемой данных

Предположим, что я имею существующий проект с текущей схемой базы данных:

![../_assets/liquibase-step-3.png](../_assets/liquibase-step-3.png)

Чтобы начать использовать liquibase, требуется выполнить `liquibase generate-changelog --changelog-file=changelog.xml`.

Содержимое сгенерированного changelog.xml:

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

Затем нужно синхронизировать сгенерированный changelog.xml файл, делается это командой `liquibase changelog-sync --changelog-file=dbchangelog.xml`.

Результатом будет синхронизация liquibase в вашем проекте:

![../_assets/liquibase-step-4.png](../_assets/liquibase-step-4.png)