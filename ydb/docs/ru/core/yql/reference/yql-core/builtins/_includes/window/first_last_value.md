## FIRST_VALUE / LAST_VALUE

Доступ к значениям из первой и последней (в порядке `ORDER BY` на окне) строк [рамки окна](../../../syntax/window.md#frame). Единственный аргумент - выражение, к которому необходим доступ.

Опционально перед `OVER` может указываться дополнительный модификатор `IGNORE NULLS`, который меняет поведение функций на первое или последнее __не пустое__ (то есть не `NULL`) значение среди строк рамки окна. Антоним этого модификатора — `RESPECT NULLS` является поведением по умолчанию и может не указываться.

**Сигнатура**
```
FIRST_VALUE(T)->T?
LAST_VALUE(T)->T?
```

**Примеры**
``` yql
SELECT
   FIRST_VALUE(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

``` yql
SELECT
   LAST_VALUE(my_column) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```
