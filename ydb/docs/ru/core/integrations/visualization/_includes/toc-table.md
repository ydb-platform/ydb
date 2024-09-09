# Визуализация данных (Business Intelligence, BI)

| Среда | Уровень поддержки  | Инструкция |
| --- | :---: | --- |
{% if ydb-datalens %}
| [{{ datalens-name }}](https://datalens.tech/ru) | Полный | [Инструкция](../datalens.md) |
{% endif %}
{% if ydb-superset %}

| [Apache Superset](https://superset.apache.org) | Через [PostgreSQL-совместимость](https://ydb.tech/docs/ru/postgresql/intro) | [Инструкция](../superset.md) |

{% endif %}
| [FineBI](https://intl.finebi.com) | Через [PostgreSQL-совместимость](../../../postgresql/intro.md) | [Инструкция](../finebi.md) |
| [Grafana](https://grafana.com) | Полный| [Инструкция](../grafana.md) |
