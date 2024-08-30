# Интеграции {{ ydb-short-name }}

В данном разделе приведена основная информация про интеграции {{ ydb-name }} со сторонними системами.

{% note info %}

В дополнение к своему собственному нативному протоколу, {{ ydb-name }} обладает слоем совместимости, что позволяет внешним системам подключаться к базам данных по сетевым протоколам [PostgreSQL](../postgresql/intro.md) или [Apache Kafka](../reference/kafka-api/index.md). Благодаря слою совместимости, множество инструментов, разработанных для работы с этими системами, могут также взаимодействовать с {{ ydb-name }}. Уровень совместимости каждого конкретного приложения необходимо уточнять отдельно.

{% endnote %}


## Клиенты с графическим интерфейсом {#gui}

 {% include notitle [Содержание](gui_clients/_includes/toc-table.md) %}


## Визуализация данных (Business Intelligence, BI) {#bi}

{% include notitle [Содержание](visualization/_includes/toc-table.md) %}


{% if ydb-airflow %}
## Оркестрация {#orchestration}

| Среда | Инструкция |
| --- | --- |
| [{{ airflow-name }}](https://airflow.apache.org) | [Инструкция](airflow.md) |

{% endif %}

## Поставка данных {#ingestion}

{% include notitle [Содержание](ingestion/_includes/toc-table.md) %}

### Потоковая поставка данных

{% include notitle [Содержание](ingestion/_includes/toc-table-streaming.md) %}

## Миграции данных {#schema_migration}

{% include notitle [Содержание](migration/_includes/toc-table.md) %}

## Смотрите также

* [{#T}](../reference/ydb-sdk/index.md)
* [{#T}](../postgresql/intro.md)
* [{#T}](../reference/kafka-api/index.md)
