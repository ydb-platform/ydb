/* syntax version 1 */
/* postgres can not */
$t = AsList(AsStruct(1 AS a), AsStruct(2 AS a), AsStruct(1 AS a));
$f = AGGREGATION_FACTORY('avg');

USE plato;

INSERT INTO @a
SELECT
    *
FROM
    as_table($t)
;

COMMIT;

SELECT
    AGGREGATE_BY(DISTINCT CAST(Unicode::ToLower(CAST(a AS Utf8) || "00"u) AS Int), $f)
FROM
    @a
;
