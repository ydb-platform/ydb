# Импорт данных из PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Данные из PostgreSQL в {{ ydb-name }} можно импортировать различными способами:

- С помощью [pg-dump](#pg-dump).
- С помощью импорта данных [из файлов](#file-import).

{% if oss == true %}

- С помощью утилиты [ydb-importer](../integrations/data-migration/import-jdbc.md).

{% endif %}

|Способ импорта|Способ работы|Сценарии использования|
|--------------|------------------|------------|
|[pg-dump](#pg-dump)|Создание всей необходимой структуры таблиц и данных|Импорт баз данных PostgreSQL целиком|
|Импорт данных [из файлов](../reference/ydb-cli/export-import/import-file.md)|Импорт файлов с данными в заранее созданные таблицы базы данных {{ydb-name}}|Импорт данных из баз данных Greenplum или любых других баз данных со сменой структуры хранения данных|
{% if oss == true %}
|[ydb-importer](../integrations/data-migration/import-jdbc.md)|Импорт данных из другой базы данных в заранее созданные таблицы базы данных {{ydb-name}}|Импорт данных из любых баз данных, поддерживающих [JDBC-протокол](https://ru.wikipedia.org/wiki/Java_Database_Connectivity)|
{% endif %}

## pg-dump {#pg-dump}

Данные из PostgreSQL в {{ ydb-short-name }} можно перенести c помощью утилит [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) и [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md). Утилита [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) устанавливается вместе с PostgreSQL, [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) — консольный клиент {{ ydb-short-name }}, который [устанавливается отдельно](../reference/ydb-cli/install.md).

Для этого нужно:

1. Сделать дамп данных через [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) со следующими параметрами:

    * `--inserts` — для добавления данных через команду [INSERT](./statements/insert_into.md), вместо использования протокола [COPY](https://www.postgresql.org/docs/current/sql-copy.html).
    * `--column-inserts` — для добавления данных через команду [INSERT](./statements/insert_into.md) с именами колонок.
    * `--rows-per-insert=1000` — для вставки данных пачками и ускорения процесса.
    * `--encoding=utf_8` — {{ ydb-short-name }} поддерживает строковые данные только в [UTF-8](https://ru.wikipedia.org/wiki/UTF-8).

2. Преобразовать дамп в [YQL](../yql/reference/index.md) с помощью [конвертера SQL-диалектов](../integrations/sql-translation/sql-dialect-converter.md) или вручную.
3. Загрузить результат в {{ ydb-short-name }} через [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) или [импорт из файлов](../reference/ydb-cli/export-import/import-file.md).

Для миграции через JDBC см. [импорт через JDBC](../integrations/data-migration/import-jdbc.md).

## Команда pg-convert {#pg-convert}

Команда `ydb tools pg-convert` **удалена** вместе с поддержкой SQL-диалекта PostgreSQL. Ранее она считывала дамп, полученный утилитой [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html), выполняла преобразования и выводила SQL-скрипт для выполнения с маркером `--!syntax_pg`.

Используйте [конвертер SQL-диалектов](../integrations/sql-translation/sql-dialect-converter.md) или ручную миграцию.

## Пример импорта дампа в {{ ydb-short-name }} {#file-import}

В качестве примера будут загружены данные, сгенерированные [pgbench](https://www.postgresql.org/docs/current/pgbench.html).

1. Поднять докер-контейнеры с PostgreSQL и {{ ydb-short-name }}:

    ```bash
    docker run --name postgres_container \
        -e POSTGRES_USER=pgroot -e POSTGRES_PASSWORD=1234 \
        -e POSTGRES_DB=local \
        -p 5433:5433 -d postgres:14 -c 'port=5433'
    docker run --name ydb-local -d --pull always -p 2136:2136 -p 8765:8765 \
        -e YDB_USE_IN_MEMORY_PDISKS=true \
        ghcr.io/ydb-platform/local-ydb:latest
    ```

    Информация по поднимаемым docker-контейнерам:

    #|
    || База данных | PostgreSQL | YDB ||
    || Имя контейнера | postgres_container | ydb-local ||
    || Адрес | postgres://pgroot:1234@localhost:5433/local | grpc://localhost:2136/local ||
    || Порт | 5433 | 2136 ||
    || Имя пользователя | pgroot | root ||
    || Пароль | 1234 | — ||
    |#

2. Сгенерировать данные через [pgbench](https://www.postgresql.org/docs/current/pgbench.html):

    ```bash
    docker exec postgres_container pgbench postgres://pgroot:1234@localhost:5433/local -i
    ```

3. Сделать дамп базы через [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html):

    ```bash
    docker exec postgres_container pg_dump postgres://pgroot:1234@localhost:5433/local --inserts \
        --column-inserts --encoding=utf_8 --rows-per-insert=1000 > dump.sql
    ```

4. Преобразовать дамп в YQL с помощью [конвертера SQL-диалектов](../integrations/sql-translation/sql-dialect-converter.md) и загрузить данные в {{ ydb-short-name }} через [импорт из файлов](../reference/ydb-cli/export-import/import-file.md) или вручную созданную схему YQL.
