/* syntax version 1 */
/* postgres can not */
SELECT
    agglist(x)
FROM (
    SELECT
        1 AS x
);

SELECT
    agglist(x)
FROM (
    SELECT
        1 AS x
    LIMIT 0
);
