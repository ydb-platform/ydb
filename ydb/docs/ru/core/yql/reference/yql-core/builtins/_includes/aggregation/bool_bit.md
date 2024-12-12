## BOOL_AND, BOOL_OR и BOOL_XOR {#bool-and-or-xor}

### Сигнатура

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Применение соответствующей логической операции (`AND`/`OR`/`XOR`) ко всем значениям булевой колонки или выражения.

Эти функции **не пропускают** `NULL` значение при агрегации и действуют по правилу `true and null == null`, `false or null == null`. Для `BOOL_AND` по всем `true` и любым `NULL` значениям превратит результат в `NULL`, а любое `false` значение превратит результат в `false` независимо от наличия `NULL`. Для `BOOL_OR` по всем `false` и любым `NULL` значениям превратит результат в `NULL`, а любое `true` значение превратит результат в `true` независимо от наличия `NULL`. Для `BOOL_XOR` любое `NULL` значение превратит результат в `NULL`. Можно увидеть примеры описанного поведения ниже.

Для агрегации с пропуском `NULL`-ов можно использовать функции `MIN`/`MAX` или `BIT_AND`/`BIT_OR`/`BIT_XOR`.

### Примеры

```yql
$data = AsList(
    AsStruct(true  AS nonNull, true AS nonFalse, NULL  AS nonTrue, true  AS anyVal),
    AsStruct(false AS nonNull, NULL AS nonFalse, NULL  AS nonTrue, NULL  AS anyVal),
    AsStruct(false AS nonNull, NULL AS nonFalse, false AS nonTrue, false AS anyVal),
);

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

## BIT_AND, BIT_OR и BIT_XOR {#bit-and-or-xor}

Применение соответствующей битовой операции ко всем значениям числовой колонки или выражения.

### Примеры

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```
