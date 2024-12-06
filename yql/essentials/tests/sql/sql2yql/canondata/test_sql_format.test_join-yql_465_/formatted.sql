PRAGMA DisableSimpleColumns;
USE plato;

SELECT
    *
FROM (
    SELECT
        *
    FROM
        a
    WHERE
        a.key > "zzz"
) AS a
JOIN
    b
ON
    a.key == b.key
;
