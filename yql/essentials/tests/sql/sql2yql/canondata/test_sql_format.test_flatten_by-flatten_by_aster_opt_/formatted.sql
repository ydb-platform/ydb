/* postgres can not */
/* syntax version 1 */
SELECT
    *
FROM (
    SELECT
        d.*,
        Just(key) AS ok
    FROM plato.Input
        AS d
)
    FLATTEN BY ok;
