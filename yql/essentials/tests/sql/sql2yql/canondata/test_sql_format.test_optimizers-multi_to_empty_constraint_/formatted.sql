/* syntax version 1 */
USE plato;

SELECT
    key,
    some(subkey)
FROM (
    SELECT
        *
    FROM
        Input
    WHERE
        key > '010' AND value IN []
)
GROUP BY
    key
UNION ALL
SELECT
    key,
    some(subkey)
FROM (
    SELECT
        *
    FROM
        Input
    WHERE
        key > '020' AND value IN []
)
GROUP BY
    key
;
