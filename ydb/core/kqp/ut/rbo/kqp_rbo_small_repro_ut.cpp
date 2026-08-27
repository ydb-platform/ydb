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

TString ExplainGenericQuery(TKikimrRunner& kikimr, const TString& query,
    const NYdb::NQuery::TExecuteQuerySettings& settings)
{
    auto result = kikimr.GetQueryClient().ExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::BeginTx().CommitTx(),
        settings
    ).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats().has_value(), "Expected explain stats");
    if (auto plan = result.GetStats()->GetPlan()) {
        return TString(*plan);
    }
    return {};
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
        const auto explainSettings = NYdb::NQuery::TExecuteQuerySettings()
            .Syntax(NYdb::NQuery::ESyntax::YqlV1)
            .ExecMode(NYdb::NQuery::EExecMode::Explain);

        // Repeat compilation so a focused perf capture contains query-planning samples.
        constexpr ui32 ExplainRuns = 10;
        for (ui32 i = 0; i < ExplainRuns; ++i) {
            const auto plan = ExplainGenericQuery(kikimr, query, explainSettings);
            UNIT_ASSERT_C(!plan.empty(), "Expected a non-empty explain plan");
        }

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(FormatResultSetYson(result.GetResultSet(0)), R"([[1]])");
    }
}

} // namespace
} // namespace NKikimr::NKqp
