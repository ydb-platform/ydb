# Перенос данных из IBM Db2 в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **IBM Db2** → {{ ydb-short-name }} через [CLI import file](../../tools/cli-import-file.md).

## Подготовка {#prerequisites}

{% include notitle [CLI import file](../../_includes/tools/cli-import-file-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Db2)

```bash
db2 "SELECT 1 FROM SYSIBM.SYSDUMMY1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/customers` (
    `cust_id` Int64,
    `name` Text,
    PRIMARY KEY (`cust_id`)
);
```

### Шаг 2. Экспорт из Db2

```bash
db2 "CONNECT TO SAMPLE USER db2inst1 USING password"
db2 "EXPORT TO /tmp/customers.csv OF DEL MODIFIED BY NOCHARDEL COLDEL, SELECT cust_id, name FROM customers"
```

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/customers --header --delimiter "," /tmp/customers.csv
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
db2 "SELECT COUNT(*) FROM customers"
```
