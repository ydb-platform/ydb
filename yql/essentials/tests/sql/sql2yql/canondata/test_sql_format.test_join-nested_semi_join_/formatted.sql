PRAGMA DisableSimpleColumns;

/* postgres can not */
USE plato;

$w1 = (
    SELECT
        u.subkey AS subkey,
        u.value AS value
    FROM
        Input1 AS u
    LEFT SEMI JOIN
        Input1 AS v
    ON
        u.key == v.key
);

$t1 = (
    SELECT
        x.value
    FROM
        Input1 AS x
    LEFT SEMI JOIN
        $w1 AS y
    ON
        x.subkey == y.subkey
);

SELECT
    *
FROM
    $t1
;
