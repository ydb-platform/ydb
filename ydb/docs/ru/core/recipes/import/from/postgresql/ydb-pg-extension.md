# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью ydb-pg-extension

Пошаговый рецепт: **PostgreSQL** → {{ ydb-short-name }} через [ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension/blob/main/docs/migration.md).

## Подготовка {#prerequisites}


## Пошаговая инструкция {#steps}

Миграция выполняется **изнутри PostgreSQL**: расширение копирует данные в {{ ydb-short-name }} параллельно, с возобновлением после сбоев.

Подробнее: [документация ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension/blob/main/docs/migration.md).

### Системные требования

* PostgreSQL с установленным расширением [ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension)
* Настроенное подключение расширения к кластеру {{ ydb-short-name }}

### Шаг 1. Установите и настройте расширение

Следуйте инструкции в репозитории ydb-pg-extension (сборка, `CREATE EXTENSION`, параметры `ydb.*` в `postgresql.conf`).

### Шаг 2. Запустите миграцию всех пользовательских таблиц

```sql
SELECT ydb_admin_migrate_to_ydb();
```

Только данные, без замены таблиц в PostgreSQL.

### Шаг 3. (Опционально) Замените таблицы на foreign tables

```sql
SELECT ydb_admin_migrate_to_ydb('replace');
```

### Миграция отдельных таблиц

```sql
SELECT ydb_admin_migrate_tables_to_ydb(ARRAY['public.users', 'public.orders']::text[]);
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
