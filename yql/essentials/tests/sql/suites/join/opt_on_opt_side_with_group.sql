PRAGMA DisableSimpleColumns;
use plato;

SELECT
    A.key as key,
    B.subkey,
    COUNT(*) AS count
FROM
    Input2 AS A
LEFT JOIN
    (
        SELECT
            key,
            CAST(key AS INT) AS subkey
        FROM
            Input3
    ) AS B
ON
    A.key == B.key
GROUP BY
    A.key, B.subkey
ORDER BY
    key
;
