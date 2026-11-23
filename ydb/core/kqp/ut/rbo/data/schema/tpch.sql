CREATE TABLE `/Root/customer` (
    c_acctbal Decimal(12, 2),
    c_address Utf8,
    c_comment Utf8,
    c_custkey Int64                     NOT NULL,           -- Identifier
    c_mktsegment Utf8,
    c_name Utf8,
    c_nationkey Int32,                                      -- FK to N_NATIONKEY
    c_phone Utf8,
    PRIMARY KEY (c_custkey)
);

CREATE TABLE `/Root/lineitem` (
    l_comment Utf8,
    l_commitdate Date,
    l_discount Decimal(12, 2),
    l_extendedprice Decimal(12, 2),
    l_linenumber Int32                  NOT NULL,
    l_linestatus Utf8,
    l_orderkey Int64                    NOT NULL,           -- FK to O_ORDERKEY
    l_partkey Int64,                                        -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Decimal(12, 2),
    l_receiptdate Date,
    l_returnflag Utf8,
    l_shipdate Date,
    l_shipinstruct Utf8,
    l_shipmode Utf8,
    l_suppkey Int64,                                        -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Decimal(12, 2),
    PRIMARY KEY (l_orderkey, l_linenumber)
);

CREATE TABLE `/Root/nation` (
    n_comment Utf8,
    n_name Utf8,
    n_nationkey Int32                   NOT NULL,           -- Identifier
    n_regionkey Int32,                                      -- FK to R_REGIONKEY
    PRIMARY KEY(n_nationkey)
);

CREATE TABLE `/Root/orders` (
    o_clerk Utf8,
    o_comment Utf8,
    o_custkey Int64,                                        -- FK to C_CUSTKEY
    o_orderdate Date,
    o_orderkey Int64                    NOT NULL,           -- Identifier
    o_orderpriority Utf8,
    o_orderstatus Utf8,
    o_shippriority Int32,
    o_totalprice Decimal(12, 2),
    PRIMARY KEY (o_orderkey)
);

CREATE TABLE `/Root/part` (
    p_brand Utf8,
    p_comment Utf8,
    p_container Utf8,
    p_mfgr Utf8,
    p_name Utf8,
    p_partkey Int64                     NOT NULL,           -- Identifier
    p_retailprice Decimal(12, 2),
    p_size Int32,
    p_type Utf8,
    PRIMARY KEY(p_partkey)
);

CREATE TABLE `/Root/partsupp` (
    ps_availqty Int32,
    ps_comment Utf8,
    ps_partkey Int64                    NOT NULL,           -- FK to P_PARTKEY
    ps_suppkey Int64                    NOT NULL,           -- FK to S_SUPPKEY
    ps_supplycost Decimal(12, 2),
    PRIMARY KEY(ps_partkey, ps_suppkey)
);

CREATE TABLE `/Root/region` (
    r_comment Utf8,
    r_name Utf8,
    r_regionkey Int32                   NOT NULL,           -- Identifier
    PRIMARY KEY(r_regionkey)
);

CREATE TABLE `/Root/supplier` (
    s_acctbal Decimal(12, 2),
    s_address Utf8,
    s_comment Utf8,
    s_name Utf8,
    s_nationkey Int32,                                      -- FK to N_NATIONKEY
    s_phone Utf8,
    s_suppkey Int64                     NOT NULL,           -- Identifier
    PRIMARY KEY(s_suppkey)
);
