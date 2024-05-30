{createExternal}

CREATE {external} TABLE `{path}/customer` (
    c_acctbal Double {notnull}, -- it should be Decimal(12, 2)
    c_address Utf8 {notnull},
    c_comment Utf8 {notnull},
    c_custkey Int64 {notnull}, -- Identifier
    c_mktsegment Utf8 {notnull},
    c_name Utf8 {notnull},
    c_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    c_phone Utf8 {notnull}
    {primary_key} (c_custkey)
)
{partition_by}(c_custkey)
WITH ({store}"{s3_prefix}/customer/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/lineitem` (
    l_comment Utf8 {notnull},
    l_commitdate Date {notnull},
    l_discount Double {notnull}, -- it should be Decimal(12, 2)
    l_extendedprice Double {notnull}, -- it should be Decimal(12, 2)
    l_linenumber Int32 {notnull},
    l_linestatus Utf8 {notnull},
    l_orderkey Int64 {notnull}, -- FK to O_ORDERKEY
    l_partkey Int64 {notnull}, -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity Double {notnull}, -- it should be Decimal(12, 2)
    l_receiptdate Date {notnull},
    l_returnflag Utf8 {notnull},
    l_shipdate Date {notnull},
    l_shipinstruct Utf8 {notnull},
    l_shipmode Utf8 {notnull},
    l_suppkey Int64 {notnull}, -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax Double {notnull} -- it should be Decimal(12, 2)
    {primary_key} (l_orderkey, l_linenumber)
)
{partition_by}(l_orderkey)
WITH ({store}"{s3_prefix}/lineitem/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/nation` (
    n_comment Utf8 {notnull},
    n_name Utf8 {notnull},
    n_nationkey Int32 {notnull}, -- Identifier
    n_regionkey Int32 {notnull} -- FK to R_REGIONKEY
    {primary_key}(n_nationkey)
)
{partition_by}(n_nationkey)

WITH ({store}"{s3_prefix}/nation/",
{partitioning} = 1
);

CREATE {external} TABLE `{path}/orders` (
    o_clerk Utf8 {notnull},
    o_comment Utf8 {notnull},
    o_custkey Int64 {notnull}, -- FK to C_CUSTKEY
    o_orderdate Date {notnull},
    o_orderkey Int64 {notnull}, -- Identifier
    o_orderpriority Utf8 {notnull},
    o_orderstatus Utf8 {notnull},
    o_shippriority Int32 {notnull},
    o_totalprice Double {notnull} -- it should be Decimal(12, 2)
    {primary_key} (o_orderkey)
)
{partition_by}(o_orderkey)
WITH ({store}"{s3_prefix}/orders/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/part` (
    p_brand Utf8 {notnull},
    p_comment Utf8 {notnull},
    p_container Utf8 {notnull},
    p_mfgr Utf8 {notnull},
    p_name Utf8 {notnull},
    p_partkey Int64 {notnull}, -- Identifier
    p_retailprice Double {notnull}, -- it should be Decimal(12, 2)
    p_size Int32 {notnull},
    p_type Utf8 {notnull}
    {primary_key}(p_partkey)
)
{partition_by}(p_partkey)
WITH ({store}"{s3_prefix}/part/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/partsupp` (
    ps_availqty Int32 {notnull},
    ps_comment Utf8 {notnull},
    ps_partkey Int64 {notnull}, -- FK to P_PARTKEY
    ps_suppkey Int64 {notnull}, -- FK to S_SUPPKEY
    ps_supplycost Double {notnull} -- it should be Decimal(12, 2)
    {primary_key}(ps_partkey, ps_suppkey)
)
{partition_by}(ps_partkey)
WITH ({store}"{s3_prefix}/partsupp/",
{partitioning} = 64
);

CREATE {external} TABLE `{path}/region` (
    r_comment Utf8 {notnull},
    r_name Utf8 {notnull},
    r_regionkey Int32 {notnull} -- Identifier
    {primary_key}(r_regionkey)
)
{partition_by}(r_regionkey)
WITH ({store}"{s3_prefix}/region/",
{partitioning} = 1
);

CREATE {external} TABLE `{path}/supplier` (
    s_acctbal Double {notnull}, -- it should be Decimal(12, 2)
    s_address Utf8 {notnull},
    s_comment Utf8 {notnull},
    s_name Utf8 {notnull},
    s_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    s_phone Utf8 {notnull},
    s_suppkey Int64 {notnull} -- Identifier
    {primary_key}(s_suppkey)
)
{partition_by}(s_suppkey)
WITH ({store}"{s3_prefix}/supplier/",
{partitioning} = 64
);
