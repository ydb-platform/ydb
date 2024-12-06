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
    ff.*,
    subkey AS sk,
    value AS val
FROM
    $req AS ff
ORDER BY
    sk
;
