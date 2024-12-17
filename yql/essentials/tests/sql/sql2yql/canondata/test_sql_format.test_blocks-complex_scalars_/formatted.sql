USE plato;

SELECT
    subkey || key AS k,
    Just(Just(1)) AS nested_opt,
    Just(1p) AS opt_pg,
    Just(Just(1p)) AS nested_opt_pg,
    2p AS pg,
    AsTuple(1, 2, Just(Just(2))) AS tuple,
    Just(Just(Just(AsTuple(1, 2, Just(Just(2)))))) AS double_tuple
FROM
    Input
ORDER BY
    k
;
