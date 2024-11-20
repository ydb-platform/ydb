PRAGMA DisableSimpleColumns;
use plato;

SELECT
    A.key,
    B.subkey
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
ORDER BY A.key
;
