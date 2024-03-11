## Поддержка диалекта YDB в инструменте миграции Liquibase. ##

### Введение ###

В современном мире разработки ПО управление версиями схемы базы данных стало критически важной задачей для обеспечения согласованности и отката изменений. Инструменты миграции базы данных, такие как Liquibase предоставляют средства для версионирования, отслеживания и применения изменений схемы.

Однако, появление новых СУБД ставит задачу интеграции с этими системами для поддержки их уникальных возможностей и диалектов SQL.

Как раз о результатах интеграции диалекта YDB c Liquibase мы поговорим в этой статье.

### Возможности диалекта YDB ###

Основной функциональностью Liquibase является абстрактное описание схемы базы данных в форматах .xml, .json, .yaml. Что обеспечивает переносимость при смене одной СУБД на другую.

В диалекте поддержаны такие основные конструкции:

1. `createTable` создание таблицы. Описание типов из SQL стандарта сопоставляется с примитивными типами YDB. К примеру тип bigint будет конвертирован в Int64. Также можно явно указывать оригинальное название, например, такие типы как Int32, Json, JsonDocument, Bytes, Interval. Но в таком случае теряется переносимость схемы.
2. `dropTable`, `alterTable`, `createIndex`, `addColumn`. Соответственно удаление таблицы, изменение, создание индексов и добавление колонки.
3. `loadData`, `loadUpdateData`. Загрузка данных из CSV файлов.

Чтобы понять, какие SQL-конструкции может выполнять YDB, ознакомьтесь с документацией по языку запросов [YQL](https://ydb.tech/docs/ru/yql/reference/).

Важно отметить, что кастомные инструукции YQL можно накатывать через нативные SQL запросы. Но YDB не стоит на месте! В дальнейшем все больше и больше классических SQL конструкций будут поддержаны.

### Как воспользоваться? ###

Есть два способа - программно из Java / Kotlin приложения или через liquibase CLI. Как воспользоваться из Java / Kotlin подробно описано в [README](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect) проекта, там же есть ссылка на пример Spring Boot приложения.

### Пример использования диалекта ### 

Для начала нужно установить саму утилиту liquibase [существующими способами](https://docs.liquibase.com/start/install/home.html). Затем нужно подложить актуальные .jar архивы [YDB JDBC драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases) и Liquibase [диалекта YDB](https://github.com/ydb-platform/ydb-java-dialects/tree/main/liquibase-dialect).

```bash
cp ydb-jdbc-driver-shaded-2.0.7.jar ./liquibase/internal/lib/
cp liquibase-ydb-dialect-0.9.6.jar ./liquibase/internal/lib/
```

Более подробное описание в разделе [Manual library management](https://docs.liquibase.com/start/install/home.html). Теперь liquibase утилитой можно пользоваться стандартными способами.
Напишем простенький liquibase.properties:

```properties
changelog-file=changelogs.xml
url=jdbc:ydb:grpc://localhost:2136/local
```

Теперь перейдем к миграции схемы данных.

<details>
<summary>Создадим первую таблицу series и добавим туда две записи.</summary>

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
</details>

<details>
<summary>Содержимое файла changelogs.xml</summary>

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
</details>

После исполнения `liquibase update` Liquibase напечатает лог исполненных миграций.

<details>
<summary>Лог</summary>

```bash
i113855673:liquibase kurdyukov-kir$ liquibase update
мар. 07, 2024 6:42:34 PM tech.ydb.jdbc.YdbDriver register
INFO: YDB JDBC Driver registered: tech.ydb.jdbc.YdbDriver@4b45dcb8
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
####################################################
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
####################################################
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
</details>

Далее в своей базе данных можно увидеть созданные Liquibase две системные таблицы: DATABASECHANGELOG, DATABASECHANGELOGLOCK.

<details>
<summary>Записи DATABASECHANGELOG</summary>

| AUTHOR | COMMENTS | CONTEXTS | DATEEXECUTED | DEPLOYMENT\_ID | DESCRIPTION | EXECTYPE | FILENAME | ID | LABELS | LIQUIBASE | MD5SUM | ORDEREXECUTED | TAG |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| kurdyukov-kir |  | null | 15:42:40 | 9826159656 | insert tableName=series; insert tableName=series | EXECUTED | migration/series.xml | added\_data\_into\_series | null | 4.25.1 | 9:cb49879b530528bc2555422bb7db58da | 2 | null |
| kurdyukov-kir | Table series. | null | 15:42:40 | 9826159656 | createTable tableName=series; createIndex indexName=series\_index, tableName=series | EXECUTED | migration/series.xml | series | null | 4.25.1 | 9:5809802102bcd74f1d8bc0f1d874463f | 1 | null |

</details>

Далее допустим, что наша схема базы данных нуждается в дополнительных миграциях.

<details>
<summary>Создадим таблицы seasons и episodes</summary>

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
</details>
<details>
<summary>Загрузим из .csv файла данные в таблицу episodes</summary>

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
</details>
<details>
<summary>Создание YDB топика</summary>

```sql
--liquibase formatted sql

--changeset kurdyukov-kir:10
CREATE TOPIC `my_topic` (
    CONSUMER my_consumer
    ) WITH (retention_period = Interval('P1D')
);
```
</details>
<details>
<summary>Содержимое changelogs.xml</summary>

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
</details>

После исполнения `liquibase update` схема базы успешно обновится.

### Поддержка и контакты ###

Если вы столкнулись с какой - то проблемой или у вас есть предложение по улучшению диалекта, то можно открыть issue в репозитории [ydb-java-dialects](https://github.com/ydb-platform/ydb-java-dialects) с тэгом `liquibase`.
