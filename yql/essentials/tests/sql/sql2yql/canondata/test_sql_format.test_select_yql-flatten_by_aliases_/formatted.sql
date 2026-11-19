PRAGMA YqlSelect = 'force';

$data = AsList(<|value: [1, 2], subkey: [3, 4]|>);

SELECT
    *
FROM
    AsTable($data) AS x
    FLATTEN BY x.value
ORDER BY
    value
;

SELECT
    *
FROM
    AsTable($data) AS x
    FLATTEN BY x.value AS item
ORDER BY
    item
;

SELECT
    *
FROM
    AsTable($data)
    FLATTEN LIST BY (
        value,
    )
ORDER BY
    value
;

SELECT
    *
FROM
    AsTable($data)
    FLATTEN LIST BY (
        value,
        subkey AS item
    )
ORDER BY
    value,
    item
;
