Use `LEFT SEMI JOIN` instead of a correlated `EXISTS` subquery and
`LEFT ONLY JOIN` instead of a correlated `NOT EXISTS` subquery. See
[Correlated subqueries, EXISTS, and NOT EXISTS](../../correlated-subqueries.md).

![JOIN](../_assets/join-YQL-06.png)

{% note info %}

`NULL` is a special value to denote nothing. Hence, `NULL` values on both sides are not treated as equal to each other. This eliminates ambiguity in some types of `JOIN` and avoids a giant Cartesian product otherwise created.

{% endnote %}
