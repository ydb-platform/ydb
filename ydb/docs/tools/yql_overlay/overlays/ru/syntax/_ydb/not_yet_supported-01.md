# Неподдерживаемые конструкции из классического SQL

## Коррелированные EXISTS и NOT EXISTS {#not-exists}

`EXISTS` и `NOT EXISTS` можно использовать с некоррелированными подзапросами. Коррелированные подзапросы не поддерживаются. Для отбора строк по наличию или отсутствию совпадающих строк используйте `LEFT SEMI JOIN` или `LEFT ONLY JOIN`. Подробнее см. в разделе [Коррелированные подзапросы, EXISTS и NOT EXISTS](../correlated-subqueries.md).

## INTERSECT и EXCEPT {#intersect-except}

YQL не поддерживает `INTERSECT` и `EXCEPT`.
