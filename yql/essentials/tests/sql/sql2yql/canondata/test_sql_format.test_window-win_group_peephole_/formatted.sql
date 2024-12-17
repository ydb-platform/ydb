/* syntax version 1 */
/* postgres can not */
$t = (
    SELECT
        'john' AS name,
        42 AS age
);

SELECT
    SUM(age) OVER w0 AS sumAge,
    LEAD(age, 1) OVER w1 AS nextAge,
    LAG(age, 1) OVER w1 AS prevAge,
    RANK() OVER w0 AS rank,
    DENSE_RANK() OVER w0 AS dense_rank,
    ROW_NUMBER() OVER w1 AS row_number,
FROM
    $t AS u
WINDOW
    w0 AS (
        ORDER BY
            name
    ),
    w1 AS ()
;
