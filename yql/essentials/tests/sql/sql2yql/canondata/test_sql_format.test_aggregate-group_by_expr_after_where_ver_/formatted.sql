SELECT
    x
FROM (
    SELECT
        1 AS x
)
WHERE
    x == 1
GROUP BY
    -x AS x
;
