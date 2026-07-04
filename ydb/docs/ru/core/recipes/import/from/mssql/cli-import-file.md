# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **Microsoft SQL Server** → {{ ydb-short-name }} через [CLI import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Экспорт из SQL Server

Через `bcp`:

```bash
bcp "SELECT id, region FROM mydb.dbo.sales" queryout /tmp/sales.csv \
  -S mssql-host -U user -P password -c -t,
```

Или через SSMS / Azure Data Studio: Export as CSV (UTF-8).

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/sales --header --delimiter "," /tmp/sales.csv
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
