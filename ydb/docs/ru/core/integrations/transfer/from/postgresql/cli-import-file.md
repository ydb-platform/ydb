# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью CLI import file

Выгрузите данные из PostgreSQL в CSV/Parquet/JSON и загрузите в уже созданную таблицу {{ ydb-short-name }} командой `ydb import file`.

Подробнее про инструмент: [import file](../../../reference/ydb-cli/export-import/import-file.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| PostgreSQL | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Универсальный путь: выгрузить данные из PostgreSQL в файл и загрузить в **уже созданную** таблицу {{ ydb-short-name }}.

Подробнее про команду: [import file](../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные из PostgreSQL в CSV

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "\copy users TO '/tmp/users.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
```

Для больших таблиц выгружайте по частям (`WHERE id BETWEEN …`) или используйте `COPY (SELECT …) TO …`.

### Шаг 3. Импортируйте файл в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/users \
  --header \
  --delimiter "," \
  /tmp/users.csv
```

Параметры `--batch-bytes` и `--max-in-flight` увеличивают пропускную способность на больших файлах.

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
