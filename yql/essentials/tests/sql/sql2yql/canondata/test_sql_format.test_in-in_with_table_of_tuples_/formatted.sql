/* postgres can not */
USE plato;
$t1 = AsList(
    AsStruct(75 AS key, 1 AS subkey),
    AsStruct(800 AS key, 2 AS subkey)
);

INSERT INTO @t1
SELECT
    *
FROM
    AS_TABLE($t1)
;
COMMIT;

$tuples = (
    SELECT
        AsTuple(key, subkey)
    FROM
        @t1
);

SELECT
    *
FROM
    Input
WHERE
    AsTuple(CAST(key AS uint64), CAST(subkey AS uint64)) IN $tuples
;
