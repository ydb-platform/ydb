/* postgres can not */
/* multirun can not */
/* syntax version 1 */
USE plato;

$t = AsList(
    AsStruct(1 AS key, 101 AS value)
);

INSERT INTO Output
SELECT
    *
FROM
    as_table($t)
ORDER BY
    key
;
