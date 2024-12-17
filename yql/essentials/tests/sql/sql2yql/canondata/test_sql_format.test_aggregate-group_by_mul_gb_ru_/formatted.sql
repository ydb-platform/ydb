/* syntax version 1 */
/* postgres can not */
SELECT
    sum(length(value)),
    vf,
    kf,
    kl,
    grouping(vf, kf, kl) AS ggg3
FROM
    plato.Input
GROUP BY
    Substring(value, 0, 1) AS vf,
    CUBE (CAST(key AS uint32) % 10u AS kl, CAST(key AS uint32) / 100u AS kf)
ORDER BY
    vf,
    kf,
    kl
;
