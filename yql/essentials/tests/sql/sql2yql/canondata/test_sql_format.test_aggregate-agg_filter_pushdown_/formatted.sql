/* syntax version 1 */
USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        subkey,
        max(value)
    FROM
        Input
    GROUP BY
        key,
        subkey
    HAVING
        count(*) < 100 AND subkey > "0"
)
WHERE
    key > "1" AND Likely(subkey < "4")
ORDER BY
    key,
    subkey
;
