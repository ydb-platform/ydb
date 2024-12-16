-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$join = (
    SELECT
        l.l_shipmode AS l_shipmode,
        o.o_orderpriority AS o_orderpriority,
        l.l_commitdate AS l_commitdate,
        l.l_shipdate AS l_shipdate,
        l.l_receiptdate AS l_receiptdate
    FROM
        plato.orders AS o
    JOIN
        plato.lineitem AS l
    ON
        o.o_orderkey == l.l_orderkey
);

$border = Date("1994-01-01");

SELECT
    l_shipmode,
    sum(
        CASE
            WHEN o_orderpriority == '1-URGENT'
            OR o_orderpriority == '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    sum(
        CASE
            WHEN o_orderpriority != '1-URGENT'
            AND o_orderpriority != '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS low_line_count
FROM
    $join
WHERE
    (l_shipmode == 'MAIL' OR l_shipmode == 'TRUCK')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND CAST(l_receiptdate AS timestamp) >= $border
    AND CAST(l_receiptdate AS timestamp) < ($border + Interval("P365D"))
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
;
