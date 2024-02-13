#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(PgCatalog) {
    Y_UNIT_TEST(PgType) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                select typname from pg_catalog.pg_type order by oid
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["bool"];["bytea"];["char"];["name"];["int8"];["int2"];["int2vector"];["int4"];
                ["regproc"];["text"];["oid"];["tid"];["xid"];["cid"];["oidvector"];["pg_ddl_command"];
                ["pg_type"];["pg_attribute"];["pg_proc"];["pg_class"];["json"];["xml"];["pg_node_tree"];
                ["table_am_handler"];["index_am_handler"];["point"];["lseg"];["path"];["box"];["polygon"];
                ["line"];["cidr"];["float4"];["float8"];["unknown"];["circle"];["macaddr8"];["money"];
                ["macaddr"];["inet"];["aclitem"];["bpchar"];["varchar"];["date"];["time"];["timestamp"];
                ["timestamptz"];["interval"];["timetz"];["bit"];["varbit"];["numeric"];["refcursor"];
                ["regprocedure"];["regoper"];["regoperator"];["regclass"];["regtype"];["record"];["cstring"];
                ["any"];["anyarray"];["void"];["trigger"];["language_handler"];["internal"];["anyelement"];
                ["_record"];["anynonarray"];["uuid"];["txid_snapshot"];["fdw_handler"];["pg_lsn"];["tsm_handler"];
                ["pg_ndistinct"];["pg_dependencies"];["anyenum"];["tsvector"];["tsquery"];["gtsvector"];
                ["regconfig"];["regdictionary"];["jsonb"];["anyrange"];["event_trigger"];["int4range"];["numrange"];
                ["tsrange"];["tstzrange"];["daterange"];["int8range"];["jsonpath"];["regnamespace"];["regrole"];
                ["regcollation"];["int4multirange"];["nummultirange"];["tsmultirange"];["tstzmultirange"];
                ["datemultirange"];["int8multirange"];["anymultirange"];["anycompatiblemultirange"];
                ["pg_brin_bloom_summary"];["pg_brin_minmax_multi_summary"];["pg_mcv_list"];["pg_snapshot"];["xid8"];
                ["anycompatible"];["anycompatiblearray"];["anycompatiblenonarray"];["anycompatiblerange"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(InformationSchema) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto db = kikimr.GetQueryClient();
        auto settings = NYdb::NQuery::TExecuteQuerySettings().Syntax(NYdb::NQuery::ESyntax::Pg);
        {
            auto result = db.ExecuteQuery(R"(
                select column_name from information_schema.columns
                order by table_schema, table_name, column_name limit 5
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["authorization_identifier"];["fdwoptions"];["fdwowner"];["foreign_data_wrapper_catalog"];
                ["foreign_data_wrapper_language"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
