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
                select count(*) from pg_catalog.pg_type
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetResultSets().empty(), "no result sets");
            CompareYson(R"([
                ["193"]
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
