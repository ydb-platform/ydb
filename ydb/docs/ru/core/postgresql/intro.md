# Совместимость {{ ydb-short-name }} с PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

[PostgreSQL](https://www.postgresql.org) совместимость – это механизм выполнения SQL запросов в PostgreSQL диалекте на инфраструктуре YDB с использованием сетевого протокола PostgreSQL. Благодаря этой возможности можно использовать привычные инструменты работы с PostgreSQL, такие, как [psql](https://www.postgresql.org/docs/14/app-psql.html) и драйвера (например, [pq](https://github.com/lib/pq) для Golang и [psycopg2](https://pypi.org/project/psycopg2/) для Python). Можно разрабатывать запросы на привычном PostgreSQL синтаксисе и получать такие преимущества YDB, как горизонтальная масштабируемость и отказоустойчивость.

PostgreSQL совместимость упрощает миграцию приложений, ранее работавших в экосистеме PostgreSQL. Сейчас поддерживается ограниченное количество инструкций и функций PostgreSQL 16. PostgreSQL совместимость позволяет переключаться с PostgreSQL на {{ydb-name}} без изменения кода проекта (в случае полной поддержки совместимостью используемых в проекте SQL-конструкций), просто изменив параметры подключения.

{% note info %}

Основной текущий сценарий использования интеграции с PostgreSQL - выполнение аналитических запросов к хранимым в {{ydb-name}} данным.

{% endnote %}

В этом разделе описываются:
- [{#T}](connect.md)
- [{#T}](interoperability.md)
- [{#T}](functions.md)
- [{#T}](pg-dump.md)
- [{#T}](advanced.md)
