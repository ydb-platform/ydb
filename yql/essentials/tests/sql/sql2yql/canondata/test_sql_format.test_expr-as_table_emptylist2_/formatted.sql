/* syntax version 1 */
/* postgres can not */
SELECT
    *
FROM
    as_table([])
    FLATTEN COLUMNS
;

SELECT
    *
FROM
    as_table([])
GROUP BY
    key,
    subkey
;

SELECT
    *
FROM
    as_table([])
    FLATTEN OPTIONAL BY (
        1 + x AS y
    )
;

SELECT DISTINCT
    *
FROM
    as_table([])
;
