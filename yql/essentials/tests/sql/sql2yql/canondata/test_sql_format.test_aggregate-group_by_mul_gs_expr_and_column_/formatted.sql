/* syntax version 1 */
/* postgres can not */
USE plato;

--insert into Output
SELECT
    count(1) AS count,
    kf,
    key,
    vf,
    vl,
    grouping(kf, key, vf, vl) AS grouping
FROM Input
GROUP BY
    GROUPING SETS (
        (CAST(key AS uint32) / 100u AS kf, key),
        (Substring(value, 0, 1) AS vf, Substring(value, 2, 1) AS vl))
ORDER BY
    kf,
    key,
    vf,
    vl;
