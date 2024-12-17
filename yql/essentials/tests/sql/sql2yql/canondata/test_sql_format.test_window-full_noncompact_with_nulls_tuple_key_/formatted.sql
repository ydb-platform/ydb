/* syntax version 1 */
/* postgres can not */
$input = (
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key == '1'
    UNION ALL
    SELECT
        NULL AS key,
        '9' AS subkey,
        '000' AS value
    UNION ALL
    SELECT
        NULL AS key,
        '9' AS subkey,
        '001' AS value
);

SELECT
    key,
    subkey,
    value,
    AGGREGATE_LIST(value) OVER w1 AS agglist1,
FROM
    $input
WINDOW
    w1 AS (
        PARTITION BY
            (key, subkey) AS pkey
        ORDER BY
            value
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
ORDER BY
    value
;
