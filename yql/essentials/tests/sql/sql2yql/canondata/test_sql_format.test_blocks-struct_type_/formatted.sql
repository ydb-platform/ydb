USE plato;

SELECT
    key,
    SOME(val) AS someVal,
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
