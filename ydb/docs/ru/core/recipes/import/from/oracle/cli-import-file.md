# Перенос данных из Oracle Database в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **Oracle Database** → {{ ydb-short-name }} через [CLI import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/employees` (
    `emp_id` Int64,
    `name` Text,
    PRIMARY KEY (`emp_id`)
);
```

### Шаг 2. Экспорт из Oracle

SQL*Plus / SQLcl:

```sql
SET MARKUP CSV ON DELIMITER ',' QUOTE ON
SET FEEDBACK OFF
SPOOL /tmp/employees.csv
SELECT emp_id, name FROM hr.employees;
SPOOL OFF
```

Или Data Pump / внешние таблицы Oracle → CSV/Parquet на общем хранилище.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/employees --header /tmp/employees.csv
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с источником:

```bash
# SQL*Plus: SELECT COUNT(*) FROM hr.employees;
```
