PRAGMA YqlSelect = 'force';

SELECT
    *
FROM
    AsTable([<|value: 'a,b'|>])
    FLATTEN BY (
        String::SplitToList(value, ',') AS item
    )
ORDER BY
    item
;

SELECT
    *
FROM
    AsTable([<|value: [1, 2], subkey: 'a,b'|>])
    FLATTEN LIST BY (
        value,
        String::SplitToList(subkey, ',') AS item
    )
ORDER BY
    value,
    item
;

SELECT
    *
FROM
    AsTable([<|value: <|items: [1, 2]|>|>]) AS x
    FLATTEN LIST BY (
        x.value.items AS item
    )
ORDER BY
    item
;

SELECT
    key,
    items
FROM
    AsTable([
        <|key: 0, items: ListCreate(Int32)|>,
        <|key: 1, items: [1, 2]|>,
        <|key: 2, items: [3]|>
    ])
    FLATTEN LIST BY (
        ListMap(items, ($value) -> ($value + 100)) AS items
    )
ORDER BY
    key,
    items
;

SELECT
    key,
    left_item,
    right_item
FROM
    AsTable([
        <|key: 0, left_items: ListCreate(Int32), right_items: [10]|>,
        <|key: 1, left_items: [1, 2], right_items: [10, 20]|>,
        <|key: 2, left_items: [3], right_items: ListCreate(Int32)|>
    ])
    FLATTEN LIST BY (
        ListMap(left_items, ($value) -> ($value + 100)) AS left_item,
        ListMap(right_items, ($value) -> ($value + 1000)) AS right_item
    )
ORDER BY
    key,
    left_item,
    right_item
;
