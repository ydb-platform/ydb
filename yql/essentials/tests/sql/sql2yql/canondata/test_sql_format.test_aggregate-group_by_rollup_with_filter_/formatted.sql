/* syntax version 1 */
/* postgres can not */
USE plato;

--insert into Output
SELECT
    key,
    subkey,
    count(*) AS total_count
FROM plato.Input
WHERE key IN ('023', '037')
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    key,
    subkey;
