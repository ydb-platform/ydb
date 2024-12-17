USE plato;

SELECT
    key,
    count(1) AS cnt,
    sum(CAST(subkey AS int32)) AS sm
FROM
    concat(Input1, Input2)
WHERE
    subkey IN ('1', '2', '3', '4')
GROUP BY
    key
ORDER BY
    sm DESC
;

SELECT
    count(1) AS cnt,
    sum(CAST(subkey AS int32)) AS sm
FROM
    concat(Input1, Input2)
;
