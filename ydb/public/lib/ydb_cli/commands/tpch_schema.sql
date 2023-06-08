CREATE TABLE `{path}customer` (
    c_acctbal Double {notnull}, -- it should be Decimal(12, 2)
    c_address String {notnull},
    c_comment String {notnull},
    c_custkey Int32 {notnull}, -- Identifier
    c_mktsegment String {notnull},
    c_name String {notnull},
    c_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    c_phone String {notnull},
    PRIMARY KEY (c_custkey)
)
{partition_customer}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;

CREATE TABLE `{path}lineitem` (
    l_comment String {notnull},
    l_commitdate Date {notnull},
    l_discount Double {notnull}, -- it should be Decimal(12, 2)
    l_extendedprice Double {notnull}, -- it should be Decimal(12, 2)
    l_linenumber Int32 {notnull},
    l_linestatus String {notnull},
    l_orderkey Int32 {notnull}, -- FK to O_ORDERKEY
    l_partkey Int32 {notnull}, -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Double {notnull}, -- it should be Decimal(12, 2)
    l_receiptdate Date {notnull},
    l_returnflag String {notnull},
    l_shipdate Date {notnull},
    l_shipinstruct String {notnull},
    l_shipmode String {notnull},
    l_suppkey Int32 {notnull}, -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Double {notnull}, -- it should be Decimal(12, 2)
    PRIMARY KEY (l_orderkey, l_linenumber)
)
{partition_lineitem}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;

CREATE TABLE `{path}nation` (
    n_comment String {notnull},
    n_name String {notnull},
    n_nationkey Int32 {notnull}, -- Identifier
    n_regionkey Int32 {notnull}, -- FK to R_REGIONKEY
    PRIMARY KEY(n_nationkey)
)
{partition_nation}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)
;

CREATE TABLE `{path}orders` (
    o_clerk String {notnull},
    o_comment String {notnull},
    o_custkey Int32 {notnull}, -- FK to C_CUSTKEY
    o_orderdate Date {notnull},
    o_orderkey Int32 {notnull}, -- Identifier
    o_orderpriority String {notnull},
    o_orderstatus String {notnull},
    o_shippriority Int32 {notnull},
    o_totalprice Double {notnull}, -- it should be Decimal(12, 2)
    PRIMARY KEY (o_orderkey)
)
{partition_orders}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;

CREATE TABLE `{path}part` (
    p_brand String {notnull},
    p_comment String {notnull},
    p_container String {notnull},
    p_mfgr String {notnull},
    p_name String {notnull},
    p_partkey Int32 {notnull}, -- Identifier
    p_retailprice Double {notnull}, -- it should be Decimal(12, 2)
    p_size Int32 {notnull},
    p_type String {notnull},
    PRIMARY KEY(p_partkey)
)
{partition_part}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;

CREATE TABLE `{path}partsupp` (
    ps_availqty Int32 {notnull},
    ps_comment String {notnull},
    ps_partkey Int32 {notnull}, -- FK to P_PARTKEY
    ps_suppkey Int32 {notnull}, -- FK to S_SUPPKEY
    ps_supplycost Double {notnull}, -- it should be Decimal(12, 2)
    PRIMARY KEY(ps_partkey, ps_suppkey)
)
{partition_partsupp}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;

CREATE TABLE `{path}region` (
    r_comment String {notnull},
    r_name String {notnull},
    r_regionkey Int32 {notnull}, -- Identifier
    PRIMARY KEY(r_regionkey)
)
{partition_region}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1)
;

CREATE TABLE `{path}supplier` (
    s_acctbal Double {notnull}, -- it should be Decimal(12, 2)
    s_address String {notnull},
    s_comment String {notnull},
    s_name String {notnull},
    s_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    s_phone String {notnull},
    s_suppkey Int32 {notnull}, -- Identifier
    PRIMARY KEY(s_suppkey)
)
{partition_supplier}
WITH ({store}
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32)
;
