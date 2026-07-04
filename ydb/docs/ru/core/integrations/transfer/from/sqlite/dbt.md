# Перенос данных из SQLite в {{ ydb-short-name }} с помощью dbt (dbt-ydb)

dbt загружает **небольшие** справочники из CSV в {{ ydb-short-name }} через механизм [seeds](https://docs.getdbt.com/docs/build/seeds). Федеративного источника для SQLite нет — для полной миграции используйте [CLI import file](cli-import-file.md) или [Spark](spark.md).

Подробнее про инструмент: [dbt](../../migration/dbt.md), [dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| SQLite | Файл базы `.db` / `.sqlite` с правами на чтение |
| Кодировка | UTF-8 для текстовых данных |

### Установка {{ ydb-short-name }} CLI

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../../reference/ydb-cli/install.md).

### Проверка подключения к {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

### Проверка доступа к источнику (SQLite)

```bash
sqlite3 /path/to/app.db "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Для SQLite **нет** федеративного источника в {{ ydb-short-name }}. dbt применим только для загрузки **небольших** справочников из CSV через [seeds](https://docs.getdbt.com/docs/build/seeds).

Подробнее: [dbt](../../migration/dbt.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/items"
```

Сравните с источником:

```bash
sqlite3 /path/to/app.db "SELECT COUNT(*) FROM items;"
```
