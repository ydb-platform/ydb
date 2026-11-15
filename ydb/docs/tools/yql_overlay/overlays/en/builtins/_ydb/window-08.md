Returns the relative position (> 0 and <= 1) of a row within a [partition](../../syntax/select/window.md#partition). No arguments.

### Signature

```yql
CUME_DIST()->Double
```

### Examples

```yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```


## SessionState() {#session-state}

A non-standard window function `SessionState()` (without arguments) lets you get the session calculation status from [SessionWindow](../../syntax/select/group-by.md#session-window) for the current row.
It's allowed only if `SessionWindow()` is present in the `PARTITION BY` section in the window definition.
