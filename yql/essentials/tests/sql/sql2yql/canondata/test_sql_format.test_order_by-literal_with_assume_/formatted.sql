/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

$t = AsList(
    AsStruct(1 AS key, 101 AS value),
    AsStruct(2 AS key, 34 AS value),
    AsStruct(4 AS key, 22 AS value),
    AsStruct(6 AS key, 256 AS value),
    AsStruct(7 AS key, 111 AS value)
);

INSERT INTO Output
SELECT
    *
FROM
    as_table($t)
ASSUME ORDER BY
    key
;
