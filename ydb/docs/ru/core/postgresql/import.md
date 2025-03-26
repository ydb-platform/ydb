# Импорт данных из PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Данные из PostgreSQL в {{ ydb-name }} можно импортировать различными способами:

- С помощью [pg-dump](#pg-dump).
- С помощью импорта данных [из файлов](#file-import).

{% if oss == true %}

- С помощью утилиты [ydb-importer](../integrations/ingestion/import-jdbc.md).

{% endif %}

|Способ импорта|Способ работы|Сценарии использования|
|--------------|------------------|------------|
|[pg-dump](#pg-dump)|Создание всей необходимой структуры таблиц и данных|Импорт баз данных PostgreSQL целиком|
|Импорт данных [из файлов](../reference/ydb-cli/export-import/import-file.md)|Импорт файлов с данными в заранее созданные таблицы базы данных {{ydb-name}}|Импорт данных из баз данных Greenplum или любых других баз данных со сменой структуры хранения данных|{% if oss == true %}
|[ydb-importer](../integrations/ingestion/import-jdbc.md)|Импорт данных из другой базы данных в заранее созданные таблицы базы данных {{ydb-name}}|Импорт данных из любых баз данных, поддерживающих [JDBC-протокол](https://ru.wikipedia.org/wiki/Java_Database_Connectivity)|{% endif %}

## pg-dump {#pg-dump}

Данные из PostgreSQL в {{ ydb-short-name }} можно перенести c помощью утилит [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html), [psql](https://www.postgresql.org/docs/current/app-psql.html) и [YDB CLI](../reference/ydb-cli/index.md). Утилиты [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) и [psql](https://www.postgresql.org/docs/current/app-psql.html) устанавливаются вместе с PostgreSQL, [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) — консольный клиент {{ ydb-short-name }}, который [устанавливается отдельно](../reference/ydb-cli/install.md).

Для этого нужно:

1. Сделать дамп данных через [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) со следующими параметрами:

    * `--inserts` — для добавления данных через команду [INSERT](./statements/insert_into.md), вместо использования протокола [COPY](https://www.postgresql.org/docs/current/sql-copy.html).
    * `--column-inserts` — для добавления данных через команду [INSERT](./statements/insert_into.md) с именами колонок.
    * `--rows-per-insert=1000` — для вставки данных пачками и ускорения процесса.
    * `--encoding=utf_8` — {{ ydb-short-name }} поддерживает строковые данные только в [UTF-8](https://ru.wikipedia.org/wiki/UTF-8).

2. Привести дамп к виду, который поддерживается {{ ydb-short-name }} командой `ydb tools pg-convert` [YDB CLI](../reference/ydb-cli/index.md).
3. Результат загрузить в {{ ydb-short-name }} в режиме postgres-совместимости.

## Команда pg-convert {#pg-convert}

Команда `ydb tools pg-convert` считывает из файла или stdin'а дамп, полученный утилитой [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html), выполняет преобразования и выводит в stdout дамп, который можно отправить в PostgreSQL-совместимую прослойку {{ ydb-short-name }}.

`ydb tools pg-convert` выполняет следующие преобразования:

* Перенос создания первичного ключа в тело команды [CREATE TABLE](./statements/create_table.md).
* Вырезание схемы `public` из имен таблиц.
* Удаление секции `WITH (...)` в `CREATE TABLE`
* Комментирование неподдерживаемых конструкций (опционально):

  * `SELECT pg_catalog.set_config.*`
  * `ALTER TABLE`

Если CLI не сможет найти первичный ключ таблицы, он автоматически создаст [BIGSERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) колонку `__ydb_stub_id` в качестве первичного ключа.

Общий вид команды:

```bash
{{ ydb-cli }} [global options...] tools pg-convert [options...]
```

* `global options` — [глобальные параметры](../reference/ydb-cli/commands/global-options.md).
* `options` — [параметры подкоманды](#options).

### Параметры подкоманды {#options}

Имя | Описание
---|---
`-i` | Имя файла, в котором находится изначальный дамп. Если опция не указана, дамп считывается из stdin'a.
`--ignore-unsupported` | При указании этой опции, неподдерживаемые конструкции будут закомментированы в итоговом дампе и продублированы в stderr. По умолчанию, при обнаружении неподдерживаемых конструкций, команда возвращает ошибку. Не относится к выражениям `ALTER TABLE`, задающим первичный ключ таблицы, они комментируются в любом случае.

{% note warning %}

При загрузке больших дампов считывание из stdin'a не рекомендуется, поскольку в таком случае весь дамп будет сохранен в оперативной памяти. Рекомендуется использовать опцию с файлом, в таком случае CLI будет держать в памяти небольшую часть дампа.

{% endnote %}

## Пример импорта дампа в {{ ydb-short-name }} {#file-import}

В качестве примера будут загружены данные, сгенерированные [pgbench](https://www.postgresql.org/docs/current/pgbench.html).

1. Поднять докер-контейнеры с PostgreSQL и {{ ydb-short-name }}:

    ```bash
    docker run --name postgres_container \
        -e POSTGRES_USER=pgroot -e POSTGRES_PASSWORD=1234 \
        -e POSTGRES_DB=local \
        -p 5433:5433 -d postgres:14 -c 'port=5433'
    docker run --name ydb-postgres -d --pull always -p 5432:5432 -p 8765:8765 \
        -e POSTGRES_USER=ydbroot -e POSTGRES_PASSWORD=4321 \
        -e YDB_FEATURE_FLAGS=enable_temp_tables,enable_table_pg_types \
        -e YDB_USE_IN_MEMORY_PDISKS=true \
        ghcr.io/ydb-platform/local-ydb:latest
    ```

    Информация по поднимаемым docker-контейнерам:

    #|
    || База данных | PostgreSQL | YDB ||
    || Имя контейнера | postgres_container | ydb-postgres ||
    || Адрес | postgres://pgroot:1234@localhost:5433/local | postgresql://ydbroot:4321@localhost:5432/local ||
    || Порт | 5433 | 5432 ||
    || Имя пользователя | pgroot | ydbroot ||
    || Пароль | 1234 | 4321 ||
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

4. Загрузить дамп базы в {{ ydb-short-name }}:

    ```bash
    ydb tools pg-convert --ignore-unsupported -i dump.sql | psql postgresql://ydbroot:4321@localhost:5432/local
    ```

    Эта команда использует {{ ydb-short-name }} CLI для преобразования файла `dump.sql` в формат, поддерживаемый {{ ydb-short-name }} в режиме совместимости с PostgreSQL. Затем преобразованный файл перенаправляется в утилиту `psql` для загрузки данных в {{ ydb-short-name }} по протоколу PostgreSQL.