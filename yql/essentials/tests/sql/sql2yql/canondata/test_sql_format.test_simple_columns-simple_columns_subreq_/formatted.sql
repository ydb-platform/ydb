/* postgres can not */
PRAGMA SimpleColumns;

USE plato;

$req = (
    SELECT
        100500 AS magic,
        t.*
    FROM
        Input AS t
);

--INSERT INTO Output
SELECT
    subkey AS sk,
    value AS val
FROM
    $req
ORDER BY
    sk
;
