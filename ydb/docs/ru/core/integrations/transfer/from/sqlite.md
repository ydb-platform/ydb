# Перенос данных из SQLite в {{ ydb-short-name }}

SQLite — встраиваемая СУБД; миграция выполняется через экспорт файла или Spark/JDBC к `.db`-файлу.

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Рабочий endpoint |
| SQLite | Файл базы `.db` / `.sqlite` с правами на чтение |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/items` (
    `id` Int64,
    `title` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Экспорт из SQLite

```bash
sqlite3 /path/to/app.db <<'EOF'
.headers on
.mode csv
.output /tmp/items.csv
SELECT id, title FROM items;
.quit
EOF
```

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/items --header /tmp/items.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.xerial:sqlite-jdbc:3.46.0.0 \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:sqlite:/path/to/app.db",
  "dbtable" -> "items",
  "driver" -> "org.sqlite.JDBC"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/items",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/items")
```

---

## dbt (dbt-ydb) — только seeds {#dbt}

Для SQLite **нет** федеративного источника в {{ ydb-short-name }}. dbt применим только для загрузки **небольших** справочников из CSV через [seeds](https://docs.getdbt.com/docs/build/seeds).

Подробнее: [dbt](../migration/dbt.md).

### Шаг 1. Экспортируйте справочник в CSV

```bash
sqlite3 /path/to/app.db -header -csv "SELECT id, title FROM items LIMIT 1000" > seeds/items.csv
```

### Шаг 2. Настройте dbt-проект

`seeds/items.csv`, в `dbt_project.yml`:

```yaml
seeds:
  my_project:
    items:
      +column_types:
        id: Int64
        title: Text
```

### Шаг 3. Загрузите seeds

```bash
pip install dbt-ydb
dbt seed
```

{% note warning %}

Seeds не предназначены для полной миграции больших SQLite-баз. Для production-объёмов используйте [CLI import file](#cli-import-file) или [Spark](#spark).

{% endnote %}

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/items"
sqlite3 /path/to/app.db "SELECT COUNT(*) FROM items;"
```
