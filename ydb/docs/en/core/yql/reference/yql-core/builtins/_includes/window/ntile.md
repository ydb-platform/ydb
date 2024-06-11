## NTILE

Distributes the rows of an ordered [partition](../../../syntax/window.md#partition) into a specified number of groups. The groups are numbered starting with one. For each row, the `NTILE` function returns the number of the group to which the row belongs.

**Signature**
```
NTILE(Uint64)->Uint64
```

**Examples**
``` yql
SELECT
    NTILE(10) OVER w AS group_num
FROM my_table
WINDOW w AS (ORDER BY key);
```

