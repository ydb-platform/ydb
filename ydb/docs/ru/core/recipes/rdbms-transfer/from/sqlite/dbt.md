# Перенос данных из SQLite в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт: **SQLite** → {{ ydb-short-name }} через [dbt](../../tools/dbt.md).

## Подготовка {#prerequisites}

{% include notitle [dbt](../../_includes/tools/dbt-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (SQLite)

```bash
sqlite3 /path/to/app.db "SELECT 1"
```

## Пошаговая инструкция {#steps}

Для SQLite **нет** федеративного источника в {{ ydb-short-name }}. dbt применим только для загрузки **небольших** справочников из CSV через [seeds](https://docs.getdbt.com/docs/build/seeds).

Подробнее: [dbt](../../../../integrations/migration/dbt.md).

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

Seeds не предназначены для полной миграции больших SQLite-баз. Для production-объёмов используйте [CLI import file](cli-import-file.md) или [Spark](spark.md).

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/items"
```

Сравните с источником:

```bash
sqlite3 /path/to/app.db "SELECT COUNT(*) FROM items;"
```
