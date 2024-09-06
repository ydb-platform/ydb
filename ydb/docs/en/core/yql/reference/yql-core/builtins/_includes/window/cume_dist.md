## CUME_DIST

Returns the relative position (> 0 and <= 1) of a row within a [partition](../../../syntax/window.md#partition). No arguments.

**Signature**
```
CUME_DIST()->Double
```


**Examples**
``` yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```

