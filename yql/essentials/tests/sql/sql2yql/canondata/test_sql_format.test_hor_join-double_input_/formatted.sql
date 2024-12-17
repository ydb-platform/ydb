/* postgres can not */
/* kikimr can not */
SELECT
    *
FROM (
    SELECT
        key,
        value || 'a' AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        key,
        '1' AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        key,
        '1' AS value
    FROM
        plato.Input
    UNION ALL
    SELECT
        key,
        '3' AS value
    FROM
        plato.Input
) AS x
ORDER BY
    key,
    value
;
