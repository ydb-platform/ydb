/* syntax version 1 */
/* postgres can not */
$x = 1.0 / length(CAST(Unicode::ToUpper("ab"u) AS String));

SELECT
    Percentile(key, $x)
FROM (
    SELECT
        1 AS key
    UNION ALL
    SELECT
        2 AS key
);
