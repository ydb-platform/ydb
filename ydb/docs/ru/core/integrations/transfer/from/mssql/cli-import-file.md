# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью CLI import file

Выгрузите данные из Microsoft SQL Server в CSV/Parquet/JSON и загрузите в уже созданную таблицу {{ ydb-short-name }} командой `ydb import file`.

Подробнее про инструмент: [import file](../../../reference/ydb-cli/export-import/import-file.md).

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

Подробнее: [import file](../../../reference/ydb-cli/export-import/import-file.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
