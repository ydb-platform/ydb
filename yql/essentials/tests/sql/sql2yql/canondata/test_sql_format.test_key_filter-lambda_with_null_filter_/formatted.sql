PRAGMA DisableSimpleColumns;

SELECT
    *
FROM
    plato.Input AS a
INNER JOIN (
    SELECT
        *
    FROM
        plato.Input
    WHERE
        key == "075"
) AS b
ON
    a.subkey == b.subkey
WHERE
    b.value != ""
;
