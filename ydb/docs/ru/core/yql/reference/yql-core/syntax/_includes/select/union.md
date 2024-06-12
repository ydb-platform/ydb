## UNION {#union}

{% note warning %}

{% include [olap_warning](../../../../../_includes/not_allow_for_olap.md) %}

{% endnote %}

Объединение результатов нескольких подзапросов с удалением дубликатов.
Поведение идентично последовательному исполнению `UNION ALL` и `SELECT DISTINCT *`.
См. [UNION ALL](#union-all) для информации о деталях поведения.

**Примеры**

```yql
SELECT key FROM T1
UNION
SELECT key FROM T2 -- возвращает таблицу различных ключей, лежащих хотя бы в одной из исходных таблиц
```
