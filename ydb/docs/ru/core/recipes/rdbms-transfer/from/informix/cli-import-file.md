# Перенос данных из IBM Informix в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **IBM Informix** → {{ ydb-short-name }} через [CLI import file](../../tools/cli-import-file.md).

## Подготовка {#prerequisites}

{% include notitle [CLI import file](../../_includes/tools/cli-import-file-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Informix)

```bash
# dbaccess: SELECT 1 FROM systables WHERE tabid=1;
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/customers` (
    `customer_num` Int64,
    `fname` Text,
    PRIMARY KEY (`customer_num`)
);
```

### Шаг 2. Экспорт из Informix

Через `dbexport` / `UNLOAD TO`:

```sql
UNLOAD TO '/tmp/customers.unl' DELIMITER ','
SELECT customer_num, fname FROM customer;
```

Преобразуйте в CSV с заголовком или укажите колонки явно при импорте.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/customers \
  --columns customer_num,fname \
  --delimiter "," \
  /tmp/customers.unl
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
# dbaccess: SELECT COUNT(*) FROM customer;
```
