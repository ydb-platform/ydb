## BOOL_AND, BOOL_OR and BOOL_XOR {#bool-and-or-xor}

### Signature

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Apply the relevant logical operation  (`AND`/`OR`/`XOR`) to all values in a Boolean column or expression.

These functions **don't skip** `NULL` during aggregation and follows the next rule: `true and null == null`, `false or null == null`. For `BOOL_AND` with any `true` values and even one `NULL` the result is `NULL`, but even one `false` changes it to `false` regardless whether `NULL` is present. For `BOOL_OR` with any `false` valse and even one `NULL` the result is `NULL`, but even one `true` changes it to `true` regardless whether `NULL` is present. `BOOL_XOR` results to `NULL` if any `NULL` is found. One can find more examples of such behaviour below.

To skip `NULL` values during aggregation, you can use the functions `MIN`/`MAX` or `BIT_AND`/`BIT_OR`/`BIT_XOR`.

### Examples

```yql
$data = [
    <|nonNull: true, nonFalse: true, nonTrue: NULL, anyVal: true|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: NULL, anyVal: NULL|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: false, anyVal: false|>,
];

SELECT
    BOOL_AND(nonNull) as nonNullAnd,      -- false
    BOOL_AND(nonFalse) as nonFalseAnd,    -- NULL
    BOOL_AND(nonTrue) as nonTrueAnd,      -- false
    BOOL_AND(anyVal) as anyAnd,           -- false
    BOOL_OR(nonNull) as nonNullOr,        -- true
    BOOL_OR(nonFalse) as nonFalseOr,      -- true
    BOOL_OR(nonTrue) as nonTrueOr,        -- NULL
    BOOL_OR(anyVal) as anyOr,             -- true
    BOOL_XOR(nonNull) as nonNullXor,      -- true
    BOOL_XOR(nonFalse) as nonFalseXor,    -- NULL
    BOOL_XOR(nonTrue) as nonTrueXor,      -- NULL
    BOOL_XOR(anyVal) as anyXor,           -- NULL
FROM AS_TABLE($data);
```

## BIT_AND, BIT_OR and BIT_XOR {#bit-and-or-xor}

Apply the relevant bitwise operation to all values of a numeric column or expression.

### Examples

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```

