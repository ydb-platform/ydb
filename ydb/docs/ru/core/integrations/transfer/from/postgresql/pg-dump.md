# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью pg_dump + pg-convert

Перенос через PostgreSQL-совместимый протокол {{ ydb-short-name }}: `pg_dump`, `ydb tools pg-convert`, `psql`.

Подробнее про инструмент: [импорт из PostgreSQL](../../../postgresql/import.md).

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

Перенос через PostgreSQL-совместимый протокол {{ ydb-short-name }}: дамп `pg_dump`, конвертация `ydb tools pg-convert`, загрузка через `psql`.

Подробнее: [импорт данных из PostgreSQL](../../../postgresql/import.md).

### Системные требования

* PostgreSQL client tools (`pg_dump`, `psql`)
* {{ ydb-short-name }} CLI
* {{ ydb-short-name }} с включённой [PostgreSQL-совместимостью](../../../postgresql/intro.md)

### Шаг 1. Сделайте дамп PostgreSQL

```bash
pg_dump "postgresql://user:password@pg-host:5432/mydb" \
  --inserts --column-inserts --encoding=utf_8 --rows-per-insert=1000 \
  -f dump.sql
```

### Шаг 2. Конвертируйте и загрузите в {{ ydb-short-name }}

```bash
ydb tools pg-convert --ignore-unsupported -i dump.sql | \
  psql "postgresql://ydbuser:ydbpass@ydb-pg-host:5432/local"
```

Замените строку подключения на endpoint PostgreSQL-совместимого интерфейса вашего кластера {{ ydb-short-name }}.

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
