PRAGMA YqlSelect = 'force';

PRAGMA AnsiInForEmptyOrNullableItemsCollections;

SELECT
    item
FROM
    AsTable([<|items: [1, 2]|>])
    FLATTEN LIST BY (
        IF(
            1 IN (
                SELECT
                    1
            ), items, []
        ) AS item
    )
ORDER BY
    item
;

SELECT
    item
FROM
    AsTable([<|items: [1, 2]|>]) AS x
    FLATTEN LIST BY (
        IF(
            x.items[0] IN (
                SELECT
                    1
            ), x.items, []
        ) AS item
    )
ORDER BY
    item
;

SELECT
    item
FROM
    AsTable([<|items: ListCreate(Int32)|>])
    FLATTEN LIST BY (
        IF(
            1 IN (
                SELECT
                    1
            ), items, []
        ) AS item
    )
ORDER BY
    item
;
