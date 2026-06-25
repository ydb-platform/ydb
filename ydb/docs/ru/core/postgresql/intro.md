# Совместимость {{ ydb-short-name }} с PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

[PostgreSQL](https://www.postgresql.org) совместимость – это механизм выполнения SQL запросов в PostgreSQL диалекте на инфраструктуре {{ ydb-short-name }} с использованием сетевого протокола PostgreSQL. Благодаря этой возможности можно использовать привычные инструменты работы с PostgreSQL, такие, как [psql](https://www.postgresql.org/docs/14/app-psql.html) и драйвера (например, [pq](https://github.com/lib/pq) для Golang и [psycopg2](https://pypi.org/project/psycopg2/) для Python). Можно разрабатывать запросы на привычном PostgreSQL синтаксисе и получать такие преимущества {{ ydb-short-name }}, как горизонтальная масштабируемость и отказоустойчивость.

По умолчанию слушатель сетевого протокола PostgreSQL (pgwire) на узлах `ydbd` **отключён**. Чтобы включить его на узле кластера, задайте `enable_local_pg_wire: true` в секции [`local_pg_wire_config`](../reference/configuration/local_pg_wire_config.md) файла конфигурации. В [локальном Docker-образе {{ ydb-short-name }}](../reference/docker/start.md) pgwire включён по умолчанию через `YDB_ENABLE_LOCAL_PGWIRE` (установите `0`, чтобы отключить). Отдельная переменная окружения `YDB_EXPERIMENTAL_PG` включает дополнительные экспериментальные feature flags совместимости с PostgreSQL и **по умолчанию выключена**.

PostgreSQL совместимость упрощает миграцию приложений, ранее работавших в экосистеме PostgreSQL. Сейчас поддерживается ограниченное количество инструкций и функций PostgreSQL 16. PostgreSQL совместимость позволяет переключаться с PostgreSQL на {{ ydb-short-name }} без изменения кода проекта (в случае полной поддержки совместимостью используемых в проекте SQL-конструкций), просто изменив параметры подключения.

В этом разделе описываются:

- [{#T}](connect.md)
- [{#T}](interoperability.md)
- [{#T}](functions.md)
- [{#T}](import.md)
