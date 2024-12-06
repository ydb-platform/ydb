/* custom error:Duplicated member: magic*/
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
    subkey AS magic, -- 'magic' is exist from ff.magic
    value AS val
FROM
    $req AS ff
ORDER BY
    sk
;
