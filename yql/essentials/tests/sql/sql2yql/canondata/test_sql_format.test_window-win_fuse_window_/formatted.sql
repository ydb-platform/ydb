/* syntax version 1 */
/* postgres can not */
USE plato;

$data = (
    SELECT
        unwrap(CAST(key AS uint32)) AS key,
        unwrap(CAST(subkey AS uint32)) AS subkey,
        value,
        value || value AS unused
    FROM
        Input4
);

INSERT INTO @data
SELECT
    *
FROM
    $data
;
COMMIT;

SELECT
    key,
    subkey,
    FIRST_VALUE(value) OVER w1 AS w1_first_value,
    SUM(subkey) OVER w2 AS w2_sum_subkey,
    LAST_VALUE(value) OVER w3 AS w3_last_value,
    SUM(key) OVER w4 AS w4_sum_key,
    LEAD(value) OVER w5 AS w5_next_value,
FROM
    @data
WINDOW
    w1 AS (
        PARTITION BY
            subkey,
            key
        ORDER BY
            value
    ),
    w2 AS (
        ORDER BY
            key,
            subkey
        ROWS UNBOUNDED PRECEDING
    ),
    w3 AS (
        PARTITION BY
            key,
            subkey
        ORDER BY
            value
        ROWS UNBOUNDED PRECEDING
    ), -- = w1
    w4 AS (
        ORDER BY
            key,
            subkey
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), -- = w2
    w5 AS (
        PARTITION BY
            subkey,
            key
        ORDER BY
            value
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) -- = w1
ORDER BY
    key,
    subkey
;
