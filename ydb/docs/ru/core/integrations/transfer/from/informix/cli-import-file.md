# Перенос данных из IBM Informix в {{ ydb-short-name }} с помощью CLI import file

Выгрузите данные из IBM Informix в CSV/Parquet/JSON и загрузите в уже созданную таблицу {{ ydb-short-name }} командой `ydb import file`.

Подробнее про инструмент: [import file](../../../reference/ydb-cli/export-import/import-file.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| IBM Informix | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (IBM Informix)

```bash
# dbaccess: SELECT 1 FROM systables WHERE tabid=1;
```

---

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../reference/ydb-cli/export-import/import-file.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
SELECT COUNT(*) FROM customer;
```
