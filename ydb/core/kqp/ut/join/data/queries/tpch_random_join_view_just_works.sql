select v.o_orderkey from 
        `/Root/tpch3_as_view` v
    join 
        `/Root/orders` as o
    on
        v.o_orderkey = o.o_custkey;