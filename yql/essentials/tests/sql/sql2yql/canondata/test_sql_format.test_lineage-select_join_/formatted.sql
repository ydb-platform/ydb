INSERT INTO plato.Output
SELECT
    a.key AS x,
    b.value AS y
FROM
    plato.Input AS a
JOIN
    plato.Input AS b
ON
    a.key == b.key
ORDER BY
    x
;
