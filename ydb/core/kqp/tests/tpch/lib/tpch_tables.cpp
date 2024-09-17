#include "tpch_tables.h"

namespace NYdb::NTpch {

using namespace NTable;

#define WITH_SECONDARY_INDEXES 0

const THashMap<TStringBuf, TTableDescription> TABLES = {
    {"nation",   TTableBuilder()
                     .AddNullableColumn("n_nationkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("n_name", EPrimitiveType::Utf8)
                     .AddNullableColumn("n_regionkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("n_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("n_nationkey")
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("nation_fk1", "n_regionkey")
#endif
                     .Build()},
    {"region",   TTableBuilder()
                     .AddNullableColumn("r_regionkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("r_name", EPrimitiveType::Utf8)
                     .AddNullableColumn("r_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("r_regionkey")
                     .Build()},
    {"part",     TTableBuilder()
                     .AddNullableColumn("p_partkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("p_name", EPrimitiveType::Utf8)
                     .AddNullableColumn("p_mfgr", EPrimitiveType::Utf8)
                     .AddNullableColumn("p_brand", EPrimitiveType::Utf8)
                     .AddNullableColumn("p_type", EPrimitiveType::Utf8)
                     .AddNullableColumn("p_size", EPrimitiveType::Int32)
                     .AddNullableColumn("p_container", EPrimitiveType::Utf8)
                     .AddNullableColumn("p_retailprice", EPrimitiveType::Double)
                     .AddNullableColumn("p_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("p_partkey")
                     .Build()},
    {"supplier", TTableBuilder()
                     .AddNullableColumn("s_suppkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("s_name", EPrimitiveType::Utf8)
                     .AddNullableColumn("s_address", EPrimitiveType::Utf8)
                     .AddNullableColumn("s_nationkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("s_phone", EPrimitiveType::Utf8)
                     .AddNullableColumn("s_acctbal", EPrimitiveType::Double)
                     .AddNullableColumn("s_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("s_suppkey")
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("supplier_fk1", "s_nationkey")
#endif
                     .Build()},
    {"partsupp", TTableBuilder()
                     .AddNullableColumn("ps_partkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("ps_suppkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("ps_availqty", EPrimitiveType::Int32)
                     .AddNullableColumn("ps_supplycost", EPrimitiveType::Double)
                     .AddNullableColumn("ps_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumns({"ps_partkey", "ps_suppkey"})
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("partsupp_fk1", "ps_suppkey")
                     .AddSecondaryIndex("partsupp_fk2", "ps_partkey")
#endif
                     .Build()},
    {"customer", TTableBuilder()
                     .AddNullableColumn("c_custkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("c_name", EPrimitiveType::Utf8)
                     .AddNullableColumn("c_address", EPrimitiveType::Utf8)
                     .AddNullableColumn("c_nationkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("c_phone", EPrimitiveType::Utf8)
                     .AddNullableColumn("c_acctbal", EPrimitiveType::Double)
                     .AddNullableColumn("c_mktsegment", EPrimitiveType::Utf8)
                     .AddNullableColumn("c_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("c_custkey")
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("customer_fk1", "c_nationkey")
#endif
                     .Build()},
    {"orders",   TTableBuilder()
                     .AddNullableColumn("o_orderkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("o_custkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("o_orderstatus", EPrimitiveType::Utf8)
                     .AddNullableColumn("o_totalprice", EPrimitiveType::Double)
                     .AddNullableColumn("o_orderdate", EPrimitiveType::Date)
                     .AddNullableColumn("o_orderpriority", EPrimitiveType::Utf8)
                     .AddNullableColumn("o_clerk", EPrimitiveType::Utf8)
                     .AddNullableColumn("o_shippriority", EPrimitiveType::Int32)
                     .AddNullableColumn("o_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumn("o_orderkey")
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("orders_fk1", "o_custkey")
#endif
                     .Build()},
    {"lineitem", TTableBuilder()
                     .AddNullableColumn("l_orderkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("l_partkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("l_suppkey", EPrimitiveType::Uint32)
                     .AddNullableColumn("l_linenumber", EPrimitiveType::Uint32)
                     .AddNullableColumn("l_quantity", EPrimitiveType::Double)
                     .AddNullableColumn("l_extendedprice", EPrimitiveType::Double)
                     .AddNullableColumn("l_discount", EPrimitiveType::Double)
                     .AddNullableColumn("l_tax", EPrimitiveType::Double)
                     .AddNullableColumn("l_returnflag", EPrimitiveType::Utf8)
                     .AddNullableColumn("l_linestatus", EPrimitiveType::Utf8)
                     .AddNullableColumn("l_shipdate", EPrimitiveType::Date)
                     .AddNullableColumn("l_commitdate", EPrimitiveType::Date)
                     .AddNullableColumn("l_receiptdate", EPrimitiveType::Date)
                     .AddNullableColumn("l_shipinstruct", EPrimitiveType::Utf8)
                     .AddNullableColumn("l_shipmode", EPrimitiveType::Utf8)
                     .AddNullableColumn("l_comment", EPrimitiveType::Utf8)
                     .SetPrimaryKeyColumns({"l_orderkey", "l_linenumber"})
#if WITH_SECONDARY_INDEXES
                     .AddSecondaryIndex("lineitem_fk1", "l_orderkey")
                     .AddSecondaryIndex("lineitem_fk2", TVector<TString>{"l_partkey", "l_suppkey"})
#endif
                     .Build()}
};

} // namespace NYdb::NTpch
