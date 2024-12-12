SELECT
    count(DISTINCT key) AS dist,
    count(key) AS full
FROM
    plato.Input2
;
