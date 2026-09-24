# WHERE

Фильтрация строк в результате выполнения `SELECT` по условию в колоночной или строковой таблице.

## Пример

```yql
SELECT key FROM my_table
WHERE value > 0;
```

{% note warning %}

Подзапрос в `WHERE` не может обращаться к колонкам или псевдонимам таблиц из
внешнего запроса. Для отбора строк, у которых есть совпадающие строки в другой
таблице, используйте [`LEFT SEMI JOIN`](../correlated-subqueries.md#exists). Для
отбора строк без совпадений используйте
[`LEFT ONLY JOIN`](../correlated-subqueries.md#not-exists).

{% endnote %}
