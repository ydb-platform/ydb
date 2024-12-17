SELECT
    c.key
FROM (
    SELECT
        b.key
    FROM (
        SELECT
            a.key
        FROM
            plato.Input AS a
    ) AS b
) AS c
ORDER BY
    c.key
;
