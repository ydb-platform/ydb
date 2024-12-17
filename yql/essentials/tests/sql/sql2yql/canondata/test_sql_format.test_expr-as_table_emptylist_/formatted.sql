/* syntax version 1 */
/* postgres can not */
SELECT
    *
FROM
    as_table([])
;

SELECT
    x + 1
FROM
    as_table([])
;

SELECT
    *
FROM
    as_table([])
ORDER BY
    x
LIMIT 5 OFFSET 2;

SELECT
    x
FROM
    as_table([])
ORDER BY
    x
;

SELECT
    count(*)
FROM
    as_table([])
;

SELECT
    x,
    count(*)
FROM
    as_table([])
GROUP BY
    x
;

SELECT
    x,
    count(*)
FROM
    as_table([])
GROUP BY
    x
HAVING
    count(x) > 1
;

SELECT
    lead(x) OVER w,
    lag(x) OVER w,
    row_number() OVER w,
    count(*) OVER w
FROM
    as_table([])
WINDOW
    w AS ()
;

SELECT
    lead(x) OVER w,
    lag(x) OVER w,
    rank() OVER w,
    denserank() OVER w,
    count(*) OVER w
FROM
    as_table([])
WINDOW
    w AS (
        ORDER BY
            x
    )
;

INSERT INTO plato.Output
SELECT
    *
FROM
    as_table([])
;
