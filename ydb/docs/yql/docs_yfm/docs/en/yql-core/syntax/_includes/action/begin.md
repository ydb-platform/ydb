## BEGIN .. END DO {#begin}

Performing an action without declaring it (anonymous action).

**Syntax**

1. `BEGIN`.
1. List of top-level expressions.
1. `END DO`.

An anonymous action can't include any parameters.

**Example**

```
DO BEGIN
    SELECT 1;
    SELECT 2 -- here and in the previous example, you might omit ';' before END
END DO
```

