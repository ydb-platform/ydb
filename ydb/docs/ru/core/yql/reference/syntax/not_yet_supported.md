# Ещё не поддерживаемые конструкции из классического SQL

## Коррелированные EXISTS и NOT EXISTS {#not-exists}

`EXISTS` и `NOT EXISTS` можно использовать с некоррелированными подзапросами.
Коррелированные подзапросы не поддерживаются. Для отбора строк по наличию или
отсутствию совпадающих строк используйте `LEFT SEMI JOIN` или `LEFT ONLY JOIN`.
Подробнее см. в разделе [Коррелированные подзапросы, EXISTS и NOT EXISTS](correlated-subqueries.md).

## INTERSECT и EXCEPT {#intersect-except}

YQL не поддерживает `INTERSECT` и `EXCEPT`.

## NATURAL JOIN {#natural-join}

Доступный альтернативный вариант — явно перечислить совпадающие с обеих сторон колонки.

## NOW() / CURRENT_TIME() {#now}

Доступный альтернативный вариант — воспользоваться функциями [CurrentUtcDate, CurrentUtcDatetime и CurrentUtcTimestamp](../builtins/basic.md#current-utc).
