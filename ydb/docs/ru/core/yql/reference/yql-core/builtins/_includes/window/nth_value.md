## NTH_VALUE

Доступ к значения из заданной строки (в порядке `ORDER BY` на окне) [рамки окна](../../../syntax/window.md#frame). Аргументы - выражение, к которому необходим доступ и номер строки, начиная с 1.

Опционально перед `OVER` может указываться дополнительный модификатор `IGNORE NULLS`, который приводит к пропуску строк с NULL в значении первого аргумента. Антоним этого модификатора — `RESPECT NULLS` является поведением по умолчанию и может не указываться.

**Сигнатура**
```
NTH_VALUE(T,N)->T?
```

**Примеры**
``` yql
SELECT
   NTH_VALUE(my_column, 2) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

``` yql
SELECT
   NTH_VALUE(my_column, 3) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```
