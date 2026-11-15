{% note warning %}

Correlated subqueries in `WHERE` are not supported: a subquery cannot refer to columns or table aliases from an outer query. To select rows that have matching rows in another table, use [`LEFT SEMI JOIN`](../../correlated-subqueries.md#exists). To select rows without matches, use [`LEFT ONLY JOIN`](../../correlated-subqueries.md#not-exists).

{% endnote %}
