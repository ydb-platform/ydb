## IS \[NOT\] NULL {#is-null}

Проверка на пустое значение (`NULL`). Так как `NULL` является особым значением, которое [ничему не равно](../../../types/optional.md#null_expr), то обычные [операторы сравнения](#comparison-operators) для этой задачи не подходят.

**Примеры**

``` yql
SELECT key FROM my_table
WHERE value IS NOT NULL;
```