/* syntax version 1 */
/* postgres can not */
USE plato;

--insert into Output
SELECT
    key,
    subkey,
    count(1) AS total_count,
    value,
    grouping(key, subkey, value) AS group_mask
FROM
    Input
GROUP BY
    GROUPING SETS (
        value,
        ROLLUP (key, subkey)
    )
ORDER BY
    group_mask,
    value,
    key,
    subkey,
    total_count
;
