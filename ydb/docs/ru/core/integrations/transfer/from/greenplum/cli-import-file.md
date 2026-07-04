# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью CLI import file

Выгрузите данные из Greenplum в CSV/Parquet/JSON и загрузите в уже созданную таблицу {{ ydb-short-name }} командой `ydb import file`.

Подробнее про инструмент: [import file](../../../reference/ydb-cli/export-import/import-file.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Greenplum | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/lineitem` (
    `l_orderkey` Int64,
    `l_linenumber` Int32,
    `l_quantity` Double,
    PRIMARY KEY (`l_orderkey`, `l_linenumber`)
);
```

### Шаг 2. Выгрузка из Greenplum

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c \
  "\copy (SELECT l_orderkey, l_linenumber, l_quantity FROM lineitem) TO '/tmp/lineitem.csv' WITH (FORMAT csv, HEADER true)"
```

Для распределённых таблиц выгрузка через master может быть медленной — используйте `gpfdist` / внешние таблицы Greenplum для параллельного экспорта в S3 или NFS, затем импортируйте файлы в {{ ydb-short-name }}.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/lineitem --header /tmp/lineitem.csv
```

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
