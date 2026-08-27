#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NTable;

constexpr TStringBuf SchemaPath = "data/repro/small/schema.sql";
constexpr TStringBuf QueryPath = "data/repro/small/query.sql";

TString ReadFixture(TStringBuf path) {
    TFileInput input(SRC_(TString(path)));
    return input.ReadAll();
}

Y_UNIT_TEST_SUITE(KqpRboSmallRepro) {
    Y_UNIT_TEST(SelectFromTableLimitUsesNewRbo) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetBackportMode(
            NKikimrConfig::TTableServiceConfig_EBackportMode_All);

        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto schemeResult = session.ExecuteSchemeQuery(ReadFixture(SchemaPath)).GetValueSync();
        UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList()
            .AddListItem()
            .BeginStruct()
            .AddMember("key").Int32(1)
            .EndStruct()
            .EndList();
        auto upsertResult = db.BulkUpsert("/Root/small-rbo/table", rows.Build()).GetValueSync();
        UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

        const TString query = ReadFixture(QueryPath);

        // Repeat compilation so a focused perf capture contains query-planning samples.
        constexpr ui32 ExplainRuns = 10;
        for (ui32 i = 0; i < ExplainRuns; ++i) {
            auto explainResult = session.ExplainDataQuery(query).GetValueSync();
            UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
        }

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1]])");
    }
}

} // namespace
} // namespace NKikimr::NKqp
