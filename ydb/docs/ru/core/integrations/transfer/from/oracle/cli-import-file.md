# Перенос данных из Oracle Database в {{ ydb-short-name }} с помощью CLI import file

Выгрузите данные из Oracle Database в CSV/Parquet/JSON и загрузите в уже созданную таблицу {{ ydb-short-name }} командой `ydb import file`.

Подробнее про инструмент: [import file](../../../reference/ydb-cli/export-import/import-file.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Oracle Database | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

---

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../reference/ydb-cli/export-import/import-file.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с источником:

```bash
SELECT COUNT(*) FROM hr.employees;
```
