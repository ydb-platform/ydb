-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date('1994-03-01');

$join = (
    SELECT
        o.o_orderpriority AS o_orderpriority,
        o.o_orderdate AS o_orderdate,
        l.l_commitdate AS l_commitdate,
        l.l_receiptdate AS l_receiptdate
    FROM
        plato.orders AS o
    JOIN ANY
        plato.lineitem AS l
    ON
        o.o_orderkey == l.l_orderkey
);

SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    $join
WHERE
    CAST(o_orderdate AS Timestamp) >= $border
    AND CAST(o_orderdate AS Timestamp) < DateTime::MakeDate(DateTime::ShiftMonths($border, 3))
    AND l_commitdate < l_receiptdate
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
;
