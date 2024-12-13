PRAGMA DisableSimpleColumns;

/* postgres can not */
USE plato;

$data1 = (
    SELECT
        CAST(key AS uint32) AS key,
        subkey,
        value
    FROM
        Input1
);

$data2 = (
    SELECT
        CAST(key AS uint32) % 100u AS key,
        subkey,
        value
    FROM
        Input2
);

--INSERT INTO Output
SELECT
    i1.*
FROM
    $data1 AS i1
LEFT SEMI JOIN
    $data2 AS i2
ON
    i1.key == i2.key
LEFT OUTER JOIN
    $data1 AS i3
ON
    i1.key == i3.key
ORDER BY
    i1.key
;
