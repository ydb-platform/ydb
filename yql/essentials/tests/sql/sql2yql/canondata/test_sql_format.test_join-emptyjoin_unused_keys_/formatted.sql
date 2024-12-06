/* syntax version 1 */
USE plato;

INSERT INTO @Input1
SELECT
    '' AS k1,
    '' AS v1,
    '' AS u1
LIMIT 0;
COMMIT;

SELECT
    v3
FROM
    @Input1 AS a
JOIN
    Input2 AS b
ON
    (a.k1 == b.k2)
RIGHT JOIN
    Input3 AS c
ON
    (a.k1 == c.k3)
ORDER BY
    v3
;
