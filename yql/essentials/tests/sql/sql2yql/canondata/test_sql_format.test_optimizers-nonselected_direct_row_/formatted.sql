USE plato;

SELECT
    key,
    subkey,
    value
FROM (
    SELECT
        TablePath() AS tbl,
        key,
        subkey,
        value
    FROM concat(Input1, Input2)
)
WHERE tbl == "Input" AND value != "";
