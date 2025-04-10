pragma DistinctOverKeys;
SELECT x, count(distinct x) AS cnt
FROM (values (1),(1)) as a(x) GROUP BY x

