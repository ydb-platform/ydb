## BETWEEN {#between}

Checking whether a value is in a range. It's equivalent to two conditions with `>=` and `<=` (range boundaries are included). Can be used with the `NOT` prefix to support inversion.

**Examples**

```yql
SELECT * FROM my_table
WHERE key BETWEEN 10 AND 20;
```

