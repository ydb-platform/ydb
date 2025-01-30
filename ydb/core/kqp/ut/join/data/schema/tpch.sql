CREATE TABLE `/Root/customer` (
    c_acctbal Double,
    c_address String,
    c_comment String,
    c_custkey Int32     NOT NULL, -- Identifier
    c_mktsegment String ,
    c_name String ,
    c_nationkey Int32 , -- FK to N_NATIONKEY
    c_phone String ,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE `/Root/lineitem` (
    l_comment String ,
    l_commitdate Date ,
    l_discount Double , -- it should be Decimal(12, 2)
    l_extendedprice Double , -- it should be Decimal(12, 2)
    l_linenumber Int32  NOT NULL,
    l_linestatus String ,
    l_orderkey Int32    NOT NULL, -- FK to O_ORDERKEY
    l_partkey Int32 , -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Double , -- it should be Decimal(12, 2)
    l_receiptdate Date ,
    l_returnflag String ,
    l_shipdate Date ,
    l_shipinstruct String ,
    l_shipmode String ,
    l_suppkey Int32 , -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (l_orderkey, l_linenumber)
);

CREATE TABLE `/Root/nation` (
    n_comment String ,
    n_name String ,
    n_nationkey Int32   NOT NULL, -- Identifier
    n_regionkey Int32 , -- FK to R_REGIONKEY
    PRIMARY KEY(n_nationkey)
);

CREATE TABLE `/Root/orders` (
    o_clerk String ,
    o_comment String ,
    o_custkey Int32 , -- FK to C_CUSTKEY
    o_orderdate Date ,
    o_orderkey Int32    NOT NULL, -- Identifier
    o_orderpriority String ,
    o_orderstatus String ,
    o_shippriority Int32 ,
    o_totalprice Double , -- it should be Decimal(12, 2)
    PRIMARY KEY (o_orderkey)
);

CREATE TABLE `/Root/part` (
    p_brand String ,
    p_comment String ,
    p_container String ,
    p_mfgr String ,
    p_name String ,
    p_partkey Int32     NOT NULL, -- Identifier
    p_retailprice Double , -- it should be Decimal(12, 2)
    p_size Int32 ,
    p_type String ,
    PRIMARY KEY(p_partkey)
);

CREATE TABLE `/Root/partsupp` (
    ps_availqty Int32 ,
    ps_comment String ,
    ps_partkey Int32    NOT NULL, -- FK to P_PARTKEY
    ps_suppkey Int32    NOT NULL, -- FK to S_SUPPKEY
    ps_supplycost Double , -- it should be Decimal(12, 2)
    PRIMARY KEY(ps_partkey, ps_suppkey)
);

CREATE TABLE `/Root/region` (
    r_comment String ,
    r_name String ,
    r_regionkey Int32       NOT NULL, -- Identifier
    PRIMARY KEY(r_regionkey)
);

CREATE TABLE `/Root/supplier` (
    s_acctbal Double , -- it should be Decimal(12, 2)
    s_address String ,
    s_comment String ,
    s_name String ,
    s_nationkey Int32 , -- FK to N_NATIONKEY
    s_phone String ,
    s_suppkey Int32     NOT NULL, -- Identifier
    PRIMARY KEY(s_suppkey)
);
