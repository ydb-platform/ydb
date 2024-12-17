/* postgres can not */
/* hybridfile can not YQL-17743 */
USE plato;

$list = AsList(AsStruct(1 AS a, '2' AS b, '3' AS c), AsStruct(4 AS a, '5' AS b, '6' AS c));

INSERT INTO Output
SELECT
    *
FROM
    as_table($list)
ORDER BY
    a DESC,
    b,
    c DESC
;
