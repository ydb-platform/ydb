# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью Федеративные запросы

Узлы {{ ydb-short-name }} читают данные из MySQL / MariaDB через External Data Source и записывают в локальную таблицу одним YQL-запросом.

Подробнее про инструмент: [импорт через федеративные запросы](../../../concepts/query_execution/federated_query/import_and_export.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| MySQL / MariaDB | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [MySQL как внешний источник](../../../concepts/query_execution/federated_query/mysql.md), [импорт](../../../concepts/query_execution/federated_query/import_and_export.md).

```yql
CREATE SECRET mysql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mysql_src WITH (
    SOURCE_TYPE="MySQL",
    LOCATION="mysql-host:3306",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mysql_password"
);

CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/orders`
SELECT * FROM mysql_src.orders;
```

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
