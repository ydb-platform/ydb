/* postgres can not */
/* kikimr can not */
PRAGMA yt.DisableOptimizers = 'UnorderedOuts';

SELECT
    *
FROM (
    SELECT
        key,
        value || 'a' AS value
    FROM
        plato.Input1
    UNION ALL
    SELECT
        key,
        '1' AS value
    FROM
        plato.Input2
    UNION ALL
    SELECT
        key,
        '2' AS value
    FROM
        plato.Input3
    UNION ALL
    SELECT
        key,
        '3' AS value
    FROM
        plato.Input4
    UNION ALL
    SELECT
        key,
        '4' AS value
    FROM
        plato.Input5
) AS x
ORDER BY
    key,
    value
;
