/* postgres can not */
USE plato;

$foo = (
    SELECT
        key,
        subkey,
        value,
        sum(CAST(subkey AS uint32)) OVER w AS sks
    FROM
        Input
    WINDOW
        w AS (
            PARTITION BY
                key
            ORDER BY
                subkey
        )
);

$bar = (
    SELECT
        key,
        subkey,
        sum(CAST(subkey AS uint32)) OVER w AS sks,
        avg(CAST(subkey AS uint32)) OVER w AS ska
    FROM
        Input4
    WINDOW
        w AS (
            PARTITION BY
                key
            ORDER BY
                subkey
        )
);

SELECT
    key,
    subkey,
    value
FROM
    $foo
ORDER BY
    key,
    subkey
;

SELECT
    key,
    ska
FROM
    $bar
ORDER BY
    key,
    ska
;
