# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **Greenplum** → {{ ydb-short-name }} через [CLI import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
