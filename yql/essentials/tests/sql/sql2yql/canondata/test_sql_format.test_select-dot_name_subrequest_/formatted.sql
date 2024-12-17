/* postgres can not */
USE plato;

PRAGMA DisableSimpleColumns;

$req = (
    SELECT
        100500 AS magic,
        t.*
    FROM
        Input AS t
);

--INSERT INTO Output
SELECT
    `t.subkey` AS sk,
    `t.value` AS val
FROM
    $req
ORDER BY
    sk
;
