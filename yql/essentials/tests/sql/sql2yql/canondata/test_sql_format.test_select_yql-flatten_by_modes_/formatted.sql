PRAGMA YqlSelect = 'force';

SELECT
    item
FROM
    AsTable([<|items: [1, 2]|>])
    FLATTEN LIST BY items AS item
ORDER BY
    item
;

SELECT
    item
FROM
    AsTable([<|items: {'one': 1, 'two': 2}|>])
    FLATTEN DICT BY items AS item
ORDER BY
    item
;

SELECT
    item
FROM
    AsTable([<|items: Just(1)|>, <|items: Nothing(Int32?)|>])
    FLATTEN OPTIONAL BY items AS item
ORDER BY
    item
;

SELECT
    item
FROM
    AsTable([<|items: [1, 2]|>])
    FLATTEN BY items AS item
ORDER BY
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 0, items: Just([1, 2])|>])
    FLATTEN LIST BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 1, items: Just(ListCreate(Int32))|>])
    FLATTEN LIST BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 2, items: Nothing(List<Int32>?)|>])
    FLATTEN LIST BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 0, items: Just({'one': 1, 'two': 2})|>])
    FLATTEN DICT BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 1, items: Just(DictCreate(String, Int32))|>])
    FLATTEN DICT BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 2, items: Nothing(Dict<String, Int32>?)|>])
    FLATTEN DICT BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 0, items: Just(0)|>])
    FLATTEN OPTIONAL BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    marker,
    item
FROM
    AsTable([<|marker: 1, items: Nothing(Int32?)|>])
    FLATTEN OPTIONAL BY items AS item
ORDER BY
    marker,
    item
;

SELECT
    item
FROM
    AsTable([<|items: Just([1, 2])|>])
    FLATTEN BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Nothing(List<Int32>?)|>])
    FLATTEN BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just({'one': 1})|>])
    FLATTEN BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Nothing(Dict<String, Int32>?)|>])
    FLATTEN BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just([1, 2])|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just(ListCreate(Int32))|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Nothing(List<Int32>?)|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just({'one': 1})|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just(DictCreate(String, Int32))|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Nothing(Dict<String, Int32>?)|>])
    FLATTEN OPTIONAL BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just(ListCreate(Int32))|>])
    FLATTEN BY items AS item
;

SELECT
    item
FROM
    AsTable([<|items: Just(DictCreate(String, Int32))|>])
    FLATTEN BY items AS item
;
