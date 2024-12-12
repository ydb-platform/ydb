## BOOL_AND, BOOL_OR and BOOL_XOR {#bool-and-or-xor}

### Signature

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Apply the relevant logical operation  (`AND`/`OR`/`XOR`) to all values in a Boolean column or expression.

These functions **don't skip** `NULL` during aggregation and follows the next rule: `true and null == null`, `false or null == null`. For `BOOL_AND` with any `true` values and even one `NULL` the result is `NULL`, but even one `false` changes it to `false` regardless whether `NULL` is present. For `BOOL_OR` with any `false` valse and even one `NULL` the result is `NULL`, but even one `true` changes it to `true` regardless whether `NULL` is present. `BOOL_XOR` results to `NULL` if any `NULL` is found.

To skip `NULLs` during aggregation, you can use the functions `MIN`/`MAX` or `BIT_AND`/`BIT_OR`/`BIT_XOR`.

### Examples

```yql
SELECT
  BOOL_AND(bool_column),
  BOOL_OR(bool_column),
  BOOL_XOR(bool_column)
FROM my_table;
```

## BIT_AND, BIT_OR and BIT_XOR {#bit-and-or-xor}

Apply the relevant bitwise operation to all values of a numeric column or expression.

### Examples

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```

