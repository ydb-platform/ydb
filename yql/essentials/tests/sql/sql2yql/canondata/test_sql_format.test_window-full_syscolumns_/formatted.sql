/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    value,
    max(value) OVER (
        PARTITION BY
            CAST(TableName() AS Utf8)
    ),
    CAST(TableName() AS Utf8),
FROM
    Input
ORDER BY
    value
;

SELECT
    value,
    max(value) OVER (
        ORDER BY
            CAST(TableName() AS Utf8)
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),
    CAST(TableName() AS Utf8),
FROM
    Input
ORDER BY
    value
;
