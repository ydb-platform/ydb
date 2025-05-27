CREATE VIEW `/Root/tpch3_as_view` WITH (security_invoker = TRUE) AS
    select
        c.c_mktsegment as c_mktsegment,
        o.o_orderdate as o_orderdate,
        o.o_shippriority as o_shippriority,
        o.o_orderkey as o_orderkey
    from
        `/Root/customer` as c
    join
        `/Root/orders` as o
    on
        c.c_custkey = o.o_custkey