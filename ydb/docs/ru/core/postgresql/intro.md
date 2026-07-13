# Совместимость с PostgreSQL (удалена)

{% note alert %}

**Эта возможность удалена.** Экспериментальная совместимость с PostgreSQL — сетевой протокол PostgreSQL (pgwire), диалект SQL PostgreSQL (`--!syntax_pg`) и связанные инструменты — больше не доступны в {{ ydb-short-name }}.

{% endnote %}

Ранее {{ ydb-short-name }} предоставлял **экспериментальный** слой совместимости с PostgreSQL. Он не предназначался для промышленной эксплуатации и удалён из сервера базы данных.

## Что удалено

Следующие возможности **больше не поддерживаются**:

| Возможность | Описание |
| --- | --- |
| **Сетевой протокол PostgreSQL (pgwire)** | Подключение через psql, pgAdmin и драйверы PostgreSQL (lib/pq, psycopg2, JDBC в режиме PostgreSQL) на порту 5432 |
| **Диалект SQL PostgreSQL** | Запросы с `--!syntax_pg`, `ydb sql --syntax pg` и настройка кластера `enable_pg_syntax` |
| **Инструменты для PostgreSQL** | `ydb tools pg-convert` и сценарии импорта через `pg_dump` в слой совместимости |
| **Системные представления в стиле PostgreSQL** | `.sys/pg_tables`, `.sys/pg_class` и связанные каталоги pgwire |

## Чем пользоваться вместо этого

| Если вы использовали | Перейдите на |
| --- | --- |
| psql, pgAdmin или драйверы PostgreSQL | [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md), [встроенный UI](../reference/embedded-ui/index.md), [{{ ydb-short-name }} SDK](../reference/ydb-sdk/index.md) или [JDBC-драйвер](https://github.com/ydb-platform/ydb-jdbc-driver) |
| Синтаксис SQL PostgreSQL | [YQL](../yql/reference/index.md) — собственный SQL-диалект {{ ydb-short-name }} |
| Импорт через `pg_dump` + `pg-convert` | [YDB tools dump/restore](../reference/ydb-cli/export-import/tools-dump.md) или [интеграции для миграции данных](../integrations/data-migration/index.md) |
| BI-инструменты через pgwire (например, FineBI) | Нативные коннекторы: [Superset](../integrations/visualization/superset.md), [Grafana](../integrations/visualization/grafana.md) или драйвер, поддерживаемый вашим инструментом |

Пошаговый обзор поддерживаемых клиентских опций — в разделе [Начало работы](../dev/getting-started.md).

## Что по-прежнему доступно

Удаление совместимости с PostgreSQL **не затрагивает** следующие отдельные возможности:

* **[Federated Query](../concepts/query_execution/federated_query/postgresql.md)** — выполнение YQL-запросов к **внешним** кластерам PostgreSQL
* **[UDF Pg::](../yql/reference/udf/list/postgres.md)** — функции и литералы в стиле PostgreSQL внутри YQL
* **Типы колонок PostgreSQL** (`pgint4`, `pgtext` и другие) — опциональные типы колонок таблиц в YQL, управляются настройкой `enable_table_pg_types`

## Вопросы и обратная связь

Если вам нужна помощь с миграцией с PostgreSQL-совместимости, обсудите ваш случай в [Discord {{ ydb-short-name }}](https://discord.gg/R5MvZTESWc) или создайте issue на [GitHub](https://github.com/ydb-platform/ydb/issues).
