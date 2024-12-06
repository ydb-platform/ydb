PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

$data = (
    SELECT
        key AS kk,
        subkey AS sk,
        value AS val
    FROM
        Input
    WHERE
        CAST(key AS uint32) / 100 > 3
);

--INSERT INTO Output
SELECT
    *
FROM
    Input
JOIN
    $data AS d
ON
    Input.subkey == d.kk
ORDER BY
    key,
    val
;
