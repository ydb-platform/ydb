/* postgres can not */
/* syntax version 1 */
PRAGMA yt.ViewIsolation = 'true';

USE plato;

SELECT
    k,
    s,
    v
FROM
    Input VIEW file_inner_udf1
UNION ALL
SELECT
    k,
    s,
    v
FROM
    Input VIEW file_inner_udf2
;
