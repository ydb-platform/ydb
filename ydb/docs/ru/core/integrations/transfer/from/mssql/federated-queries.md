# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью Федеративные запросы

Узлы {{ ydb-short-name }} читают данные из Microsoft SQL Server через External Data Source и записывают в локальную таблицу одним YQL-запросом.

Подробнее про инструмент: [импорт через федеративные запросы](../../../concepts/query_execution/federated_query/import_and_export.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Microsoft SQL Server | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd или SSMS: SELECT 1
```

---

## Пошаговая инструкция {#steps}

Подробнее: [Microsoft SQL Server](../../../concepts/query_execution/federated_query/ms_sql_server.md).

```yql
CREATE SECRET mssql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mssql_src WITH (
    SOURCE_TYPE="MsSQLServer",
    LOCATION="mssql-host:1433",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mssql_password",
    USE_TLS="TRUE"
);

CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/sales`
SELECT * FROM mssql_src.sales;
```

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
