SELECT
    b.sortkey
FROM (
    SELECT
        a.key AS sortkey
    FROM plato.Input
        AS a
)
    AS b
ORDER BY
    b.sortkey DESC;
