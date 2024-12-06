/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA EmitAggApply;
PRAGMA yt.UseAggPhases = "1";

SELECT
    key,
    count(value)
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
