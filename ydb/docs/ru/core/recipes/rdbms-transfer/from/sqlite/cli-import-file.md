# Перенос данных из SQLite в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **SQLite** → {{ ydb-short-name }} через [CLI import file](../../tools/cli-import-file.md).

## Подготовка {#prerequisites}

{% include notitle [CLI import file](../../_includes/tools/cli-import-file-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (SQLite)

```bash
sqlite3 /path/to/app.db "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/items"
```

Сравните с источником:

```bash
sqlite3 /path/to/app.db "SELECT COUNT(*) FROM items;"
```
