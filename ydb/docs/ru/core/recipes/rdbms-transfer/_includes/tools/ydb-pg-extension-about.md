## ydb-pg-extension {#about}

**ydb-pg-extension** — расширение PostgreSQL, которое копирует данные из PG-таблиц в {{ ydb-short-name }} параллельно, с возобновлением. Миграция запускается **SQL-командами внутри PostgreSQL**, без pgwire-совместимости на стороне {{ ydb-short-name }}.

### Системные требования

| Компонент | Требование |
| --- | --- |
| PostgreSQL | Сервер с возможностью установки C-расширения |
| {{ ydb-short-name }} | Endpoint, доступный с хоста PostgreSQL |
| Права | Суперпользователь или роль для `CREATE EXTENSION` |

### Установка

Сборка, `CREATE EXTENSION`, параметры `ydb.*` в `postgresql.conf` — в [документации ydb-pg-extension](https://github.com/ydb-platform/ydb-pg-extension/blob/main/docs/migration.md).
