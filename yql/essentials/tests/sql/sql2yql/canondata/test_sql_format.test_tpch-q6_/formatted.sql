-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date('1995-01-01');

SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    plato.lineitem
WHERE
    CAST(l_shipdate AS Timestamp) >= $border
    AND CAST(l_shipdate AS Timestamp) < ($border + Interval('P365D'))
    AND l_discount BETWEEN 0.07 - 0.01 AND 0.07 + 0.01
    AND l_quantity < 25
;
