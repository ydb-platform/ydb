## MAX_OF, MIN_OF, GREATEST, and LEAST {#max-min}

Returns the minimum or maximum among N arguments. Those functions let you replace the SQL standard statement `CASE WHEN a < b THEN a ELSE b END` that would be too sophisticated for N more than two.

The argument types must be mutually castable and accept `NULL`.

`GREATEST` is a synonym for `MAX_OF` and `LEAST` is a synonym for `MIN_OF`.

**Examples**

```yql
SELECT MIN_OF(1, 2, 3);
```

