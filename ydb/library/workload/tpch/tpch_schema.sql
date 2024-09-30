{createExternal}

CREATE {external} TABLE `{path}/customer` (
    c_acctbal {float_type} {notnull}, -- it should be Decimal(12, 2)
    c_address {string_type} {notnull},
    c_comment {string_type} {notnull},
    c_custkey Int64 {notnull}, -- Identifier
    c_mktsegment {string_type} {notnull},
    c_name {string_type} {notnull},
    c_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    c_phone {string_type} {notnull}
    {primary_key} (c_custkey)
)
{partition_by}(c_custkey)
WITH ({store}"{s3_prefix}/customer/"
{partitioning} = 64
);

CREATE {external} TABLE `{path}/lineitem` (
    l_comment {string_type} {notnull},
    l_commitdate {date_type} {notnull},
    l_discount {float_type} {notnull}, -- it should be Decimal(12, 2)
    l_extendedprice {float_type} {notnull}, -- it should be Decimal(12, 2)
    l_linenumber Int32 {notnull},
    l_linestatus {string_type} {notnull},
    l_orderkey Int64 {notnull}, -- FK to O_ORDERKEY
    l_partkey Int64 {notnull}, -- FK to P_PARTKEY, first part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_SUPPKEY
    l_quantity {float_type} {notnull}, -- it should be Decimal(12, 2)
    l_receiptdate {date_type} {notnull},
    l_returnflag {string_type} {notnull},
    l_shipdate {date_type} {notnull},
    l_shipinstruct {string_type} {notnull},
    l_shipmode {string_type} {notnull},
    l_suppkey Int64 {notnull}, -- FK to S_SUPPKEY, second part of the compound FK to (PS_PARTKEY, PS_SUPPKEY) with L_PARTKEY
    l_tax {float_type} {notnull} -- it should be Decimal(12, 2)
    {primary_key} (l_orderkey, l_linenumber)
)
{partition_by}(l_orderkey)
WITH ({store}"{s3_prefix}/lineitem/"
{partitioning} = 64
);

CREATE {external} TABLE `{path}/nation` (
    n_comment {string_type} {notnull},
    n_name {string_type} {notnull},
    n_nationkey Int32 {notnull}, -- Identifier
    n_regionkey Int32 {notnull} -- FK to R_REGIONKEY
    {primary_key}(n_nationkey)
)
{partition_by}(n_nationkey)

WITH ({store}"{s3_prefix}/nation/"
{partitioning} = 1
);

CREATE {external} TABLE `{path}/orders` (
    o_clerk {string_type} {notnull},
    o_comment {string_type} {notnull},
    o_custkey Int64 {notnull}, -- FK to C_CUSTKEY
    o_orderdate {date_type} {notnull},
    o_orderkey Int64 {notnull}, -- Identifier
    o_orderpriority {string_type} {notnull},
    o_orderstatus {string_type} {notnull},
    o_shippriority Int32 {notnull},
    o_totalprice {float_type} {notnull} -- it should be Decimal(12, 2)
    {primary_key} (o_orderkey)
)
{partition_by}(o_orderkey)
WITH ({store}"{s3_prefix}/orders/"
{partitioning} = 64
);

CREATE {external} TABLE `{path}/part` (
    p_brand {string_type} {notnull},
    p_comment {string_type} {notnull},
    p_container {string_type} {notnull},
    p_mfgr {string_type} {notnull},
    p_name {string_type} {notnull},
    p_partkey Int64 {notnull}, -- Identifier
    p_retailprice {float_type} {notnull}, -- it should be Decimal(12, 2)
    p_size Int32 {notnull},
    p_type {string_type} {notnull}
    {primary_key}(p_partkey)
)
{partition_by}(p_partkey)
WITH ({store}"{s3_prefix}/part/"
{partitioning} = 64
);

CREATE {external} TABLE `{path}/partsupp` (
    ps_availqty Int32 {notnull},
    ps_comment {string_type} {notnull},
    ps_partkey Int64 {notnull}, -- FK to P_PARTKEY
    ps_suppkey Int64 {notnull}, -- FK to S_SUPPKEY
    ps_supplycost {float_type} {notnull} -- it should be Decimal(12, 2)
    {primary_key}(ps_partkey, ps_suppkey)
)
{partition_by}(ps_partkey)
WITH ({store}"{s3_prefix}/partsupp/"
{partitioning} = 64
);

CREATE {external} TABLE `{path}/region` (
    r_comment {string_type} {notnull},
    r_name {string_type} {notnull},
    r_regionkey Int32 {notnull} -- Identifier
    {primary_key}(r_regionkey)
)
{partition_by}(r_regionkey)
WITH ({store}"{s3_prefix}/region/"
{partitioning} = 1
);

CREATE {external} TABLE `{path}/supplier` (
    s_acctbal {float_type} {notnull}, -- it should be Decimal(12, 2)
    s_address {string_type} {notnull},
    s_comment {string_type} {notnull},
    s_name {string_type} {notnull},
    s_nationkey Int32 {notnull}, -- FK to N_NATIONKEY
    s_phone {string_type} {notnull},
    s_suppkey Int64 {notnull} -- Identifier
    {primary_key}(s_suppkey)
)
{partition_by}(s_suppkey)
WITH ({store}"{s3_prefix}/supplier/"
{partitioning} = 64
);
