## NTH_VALUE

Access a value from a given row (in the window's `ORDER BY` order) [window frame](../../../syntax/window.md#frame). Arguments - the expression to access and the row number, starting with 1.

Optionally, `IGNORE NULLS` modifier can be specified before `OVER`, which causes rows with NULL in the value of the first argument to be skipped. The antonym of this modifier is `RESPECT NULLS`, which is the default behavior and may not be specified.

**Signature**
```
NTH_VALUE(T,N)->T?
```

**Examples**
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


