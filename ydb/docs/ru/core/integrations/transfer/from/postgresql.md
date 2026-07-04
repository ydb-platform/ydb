# Перенос данных из PostgreSQL в {{ ydb-short-name }}

Статья описывает полный цикл переноса **данных** из PostgreSQL в {{ ydb-short-name }}. Схему целевых таблиц нужно подготовить заранее (или использовать инструмент, который создаёт её автоматически — ydb-importer, pg_dump, ydb-pg-extension).

## Общая подготовка {#prerequisites}

### Системные требования

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| PostgreSQL | Сетевой доступ с хоста, где запускается миграция |
| Сеть | PostgreSQL должен принимать подключения с хоста миграции; для федеративных запросов — с узлов {{ ydb-short-name }} |
| Кодировка | UTF-8 для текстовых данных |

### Установка {{ ydb-short-name }} CLI

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../reference/ydb-cli/install.md).

### Проверка подключения к {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

### Проверка подключения к PostgreSQL

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

---

## CLI import file {#cli-import-file}

Универсальный путь: выгрузить данные из PostgreSQL в файл и загрузить в **уже созданную** таблицу {{ ydb-short-name }}.

Подробнее про команду: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные из PostgreSQL в CSV

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "\copy users TO '/tmp/users.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
```

Для больших таблиц выгружайте по частям (`WHERE id BETWEEN …`) или используйте `COPY (SELECT …) TO …`.

### Шаг 3. Импортируйте файл в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/users \
  --header \
  --delimiter "," \
  /tmp/users.csv
```

Параметры `--batch-bytes` и `--max-in-flight` увеличивают пропускную способность на больших файлах.

---

## Spark + ydb-spark-connector {#spark}

Подходит для больших объёмов и параллельного чтения через JDBC.

Подробнее: [{{ spark-name }}](../query-engines/spark.md), [репозиторий ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

### Системные требования

* Apache Spark 3.x
* JDK 11+
* JDBC-драйвер PostgreSQL (`org.postgresql:postgresql`)

### Шаг 1. Запустите Spark с коннекторами

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.executor.memory=4g
```

### Шаг 2. Прочитайте таблицу PostgreSQL и запишите в {{ ydb-short-name }}

{% list tabs %}

- Scala

  ```scala
  val pgUrl = "jdbc:postgresql://pg-host:5432/mydb"
  val pgOpts = Map(
    "url" -> pgUrl,
    "dbtable" -> "public.users",
    "user" -> "user",
    "password" -> "password",
    "driver" -> "org.postgresql.Driver"
  )
  val df = spark.read.format("jdbc").options(pgOpts).load()

  val ydbOpts = Map(
    "url" -> "grpc://localhost:2136",
    "database" -> "/local",
    "table" -> "mydb/users",
    "auth.mode" -> "NONE"
  )
  df.write.format("ydb").options(ydbOpts).mode("append").save("mydb/users")
  ```

- Python

  ```python
  pg_df = spark.read.format("jdbc").options(
      url="jdbc:postgresql://pg-host:5432/mydb",
      dbtable="public.users",
      user="user",
      password="password",
      driver="org.postgresql.Driver",
  ).load()

  pg_df.write.format("ydb").options(
      url="grpc://localhost:2136",
      database="/local",
      table="mydb/users",
      **{"auth.mode": "NONE"},
  ).mode("append").save("mydb/users")
  ```

{% endlist %}

{% note info %}

При отсутствии целевой таблицы Spark Connector может создать её автоматически — см. [опции автосоздания](../query-engines/spark.md).

{% endnote %}

---

## Федеративные запросы {#federated-queries}

Данные читаются с PostgreSQL напрямую узлами {{ ydb-short-name }} и записываются в локальную таблицу одним YQL-запросом.

Подробнее: [импорт через федеративные запросы](../../concepts/query_execution/federated_query/import_and_export.md), [PostgreSQL как внешний источник](../../concepts/query_execution/federated_query/postgresql.md).

{% note warning %}

Федеративные коннекторы к PostgreSQL находятся в стадии Preview. На кластере должен быть включён feature flag `enable_external_data_sources`.

{% endnote %}

### Шаг 1. Создайте секрет с паролем

```yql
CREATE SECRET pg_password WITH (value = "secret");
```

### Шаг 2. Создайте External Data Source

```yql
CREATE EXTERNAL DATA SOURCE pg_src WITH (
    SOURCE_TYPE="PostgreSQL",
    LOCATION="pg-host:5432",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="pg_password",
    PROTOCOL="NATIVE",
    SCHEMA="public"
);
```

### Шаг 3. Создайте целевую таблицу и импортируйте данные

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/users`
SELECT * FROM pg_src.users;
```

Для колоночных таблиц `UPSERT` выполняется параллельно.

---

## dbt (dbt-ydb) {#dbt}

dbt материализует SQL-модели в {{ ydb-short-name }}. Источник — External Data Source (см. раздел [Федеративные запросы](#federated-queries)).

Подробнее: [интеграция dbt](../migration/dbt.md), [репозиторий dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

### Системные требования

* Python 3.10+
* dbt Core 1.8+ (не dbt Fusion 2.0)

### Шаг 1. Установите dbt-ydb

```bash
pip install dbt-ydb
```

### Шаг 2. Настройте профиль `~/.dbt/profiles.yml`

```yaml
ydb_pg_transfer:
  target: dev
  outputs:
    dev:
      type: ydb
      host: localhost
      port: 2136
      database: /local
      schema: mydb
```

### Шаг 3. Создайте модель `models/users.sql`

```sql
{{ config(materialized='table', primary_key='id') }}

SELECT id, name
FROM pg_src.users
```

Предварительно создайте External Data Source `pg_src` (см. выше).

### Шаг 4. Запустите материализацию

```bash
dbt debug
dbt run --select users
```

---

## ydb-importer {#ydb-importer}

Прямое JDBC-подключение к PostgreSQL и параллельная загрузка в {{ ydb-short-name }} через Bulk Upsert.

Подробнее: [импорт из JDBC](../data-migration/import-jdbc.md), [пример конфигурации PostgreSQL](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-postgres.xml).

### Системные требования

* JDK 8+ (OpenJDK 8+)
* Архив [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* JDBC-драйвер PostgreSQL (`postgresql-*.jar` в каталоге `lib/`)

### Шаг 1. Распакуйте дистрибутив и положите JDBC-драйвер

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp postgresql-*.jar ~/ydb-importer/lib/
```

### Шаг 2. Подготовьте `postgres-import.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers><pool size="4"/></workers>
    <source type="postgresql">
        <jdbc-class>org.postgresql.Driver</jdbc-class>
        <jdbc-url>jdbc:postgresql://pg-host:5432/mydb</jdbc-url>
        <username>user</username>
        <password>password</password>
    </source>
    <target type="ydb">
        <connection-string>grpc://localhost:2136?database=/local</connection-string>
        <auth-mode>NONE</auth-mode>
        <replace-existing>false</replace-existing>
        <load-data>true</load-data>
    </target>
    <table-options name="default">
        <table-name-format>pg/${schema}/${table}</table-name-format>
    </table-options>
    <table-map options="default">
        <include-schemas regexp="true">public</include-schemas>
    </table-map>
</ydb-importer>
```

### Шаг 3. Запустите импорт

```bash
cd ~/ydb-importer
./ydb-importer.sh postgres-import.xml
```

---

## pg_dump + pg-convert {#pg-dump}

Перенос через PostgreSQL-совместимый протокол {{ ydb-short-name }}: дамп `pg_dump`, конвертация `ydb tools pg-convert`, загрузка через `psql`.

Подробнее: [импорт данных из PostgreSQL](../../postgresql/import.md).

### Системные требования

* PostgreSQL client tools (`pg_dump`, `psql`)
* {{ ydb-short-name }} CLI
* {{ ydb-short-name }} с включённой [PostgreSQL-совместимостью](../../postgresql/intro.md)

### Шаг 1. Сделайте дамп PostgreSQL

```bash
pg_dump "postgresql://user:password@pg-host:5432/mydb" \
  --inserts --column-inserts --encoding=utf_8 --rows-per-insert=1000 \
  -f dump.sql
```

### Шаг 2. Конвертируйте и загрузите в {{ ydb-short-name }}

```bash
ydb tools pg-convert --ignore-unsupported -i dump.sql | \
  psql "postgresql://ydbuser:ydbpass@ydb-pg-host:5432/local"
```

Замените строку подключения на endpoint PostgreSQL-совместимого интерфейса вашего кластера {{ ydb-short-name }}.

---

## ydb-pg-extension {#ydb-pg-extension}

Миграция выполняется **изнутри PostgreSQL**: расширение копирует данные в {{ ydb-short-name }} параллельно, с возобновлением после сбоев.

Подробнее: [документация ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension/blob/main/docs/migration.md).

### Системные требования

* PostgreSQL с установленным расширением [ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension)
* Настроенное подключение расширения к кластеру {{ ydb-short-name }}

### Шаг 1. Установите и настройте расширение

Следуйте инструкции в репозитории ydb-pg-extension (сборка, `CREATE EXTENSION`, параметры `ydb.*` в `postgresql.conf`).

### Шаг 2. Запустите миграцию всех пользовательских таблиц

```sql
SELECT ydb_admin_migrate_to_ydb();
```

Только данные, без замены таблиц в PostgreSQL.

### Шаг 3. (Опционально) Замените таблицы на foreign tables

```sql
SELECT ydb_admin_migrate_to_ydb('replace');
```

### Миграция отдельных таблиц

```sql
SELECT ydb_admin_migrate_tables_to_ydb(ARRAY['public.users', 'public.orders']::text[]);
```

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с PostgreSQL:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
