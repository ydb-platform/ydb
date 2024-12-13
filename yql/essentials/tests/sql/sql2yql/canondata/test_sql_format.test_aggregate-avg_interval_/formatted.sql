/* syntax version 1 */
DISCARD SELECT
    EnsureType(avg(CAST(key AS Interval)), Interval?)
FROM
    plato.Input
;
