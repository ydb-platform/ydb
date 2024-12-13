## BOOL_AND, BOOL_OR и BOOL_XOR {#bool-and-or-xor}

### Сигнатура

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Применение соответствующей логической операции (`AND`/`OR`/`XOR`) ко всем значениям булевой колонки или выражения.

В отличие от большинства агрегатных функций, эти функции **не пропускают** `NULL` значение при агрегации и действуют по правилу:

- `true AND null == null`
- `false OR null == null`

Для `BOOL_AND`:

- Для любого количества значений `true` и хотя бы одного `NULL` значения, результатом будет `NULL`.
- В случае хотя бы одного `false` значения, результатом будет `false`, независимо от наличия `NULL`.

Для `BOOL_OR`:

- Для любого количества значений `false` и хотя бы одного `NULL` значения, результатом будет `NULL`.
- В случае хотя бы одного `true` значения, результатом будет `true`, независимо от наличия `NULL`.

Для `BOOL_XOR`:

- В случае хотя бы одного `NULL` значения, результатом будет `NULL`.

Можно увидеть примеры описанного поведения ниже.

Для агрегации с пропуском `NULL`-ов можно использовать функции `MIN`/`MAX` или `BIT_AND`/`BIT_OR`/`BIT_XOR`.

### Примеры

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

## BIT_AND, BIT_OR и BIT_XOR {#bit-and-or-xor}

Применение соответствующей битовой операции ко всем значениям числовой колонки или выражения.

### Примеры

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```
