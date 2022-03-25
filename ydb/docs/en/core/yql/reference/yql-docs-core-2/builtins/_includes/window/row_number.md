## ROW_NUMBER {#row_number}

Row number within a [partition](../../../syntax/window.md#partition). No arguments.

**Examples**

```yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```

