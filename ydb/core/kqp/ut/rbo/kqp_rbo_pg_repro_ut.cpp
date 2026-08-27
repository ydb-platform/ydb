#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

#include <util/stream/file.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NTable;

constexpr TStringBuf SchemaPath = "data/repro/pg_rbo/schema.sql";
constexpr TStringBuf QueryPath = "data/repro/pg_rbo/query.sql";

TString ReadFixture(TStringBuf path) {
    TFileInput input(SRC_(TString(path)));
    return input.ReadAll();
}

void MakeDirectory(TKikimrRunner& kikimr, const TString& path) {
    auto result = kikimr.GetSchemeClient().MakeDirectory(path).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), path << ": " << result.GetIssues().ToString());
}

void CreateSchema(TSession& session) {
    const TString schema = ReadFixture(SchemaPath);
    const auto settings = TExecSchemeQuerySettings()
        .ClientTimeout(TDuration::Minutes(10))
        .OperationTimeout(TDuration::Minutes(10))
        .CancelAfter(TDuration::Minutes(10));

    size_t begin = 0;
    ui32 statementNumber = 0;
    while (begin < schema.size()) {
        const size_t end = schema.find(';', begin);
        if (end == TString::npos) {
            break;
        }

        TString statement = schema.substr(begin, end - begin + 1);
        begin = end + 1;
        if (statement.find("CREATE TABLE") == TString::npos) {
            continue;
        }

        ++statementNumber;
        auto result = session.ExecuteSchemeQuery(statement, settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(),
            "Schema statement #" << statementNumber << " failed: "
            << result.GetIssues().ToString() << "\n" << statement);
    }

    UNIT_ASSERT_VALUES_EQUAL(statementNumber, 8);
}

std::pair<ui64, ui64> GetNewRboCompileCounters(TKikimrRunner& kikimr) {
    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    return {
        counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Success")->Val(),
        counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Failed")->Val(),
    };
}

Y_UNIT_TEST_SUITE(KqpRboPgRepro) {
    Y_UNIT_TEST(PostgresShapedJoinQueryUsesNewRbo) {
        constexpr ui32 InfiniteTimeoutMs = std::numeric_limits<ui32>::max();
        const auto infiniteTimeout = TDuration::MilliSeconds(InfiniteTimeoutMs);

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(true);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
        appConfig.MutableTableServiceConfig()->SetBackportMode(
            NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        appConfig.MutableTableServiceConfig()->SetCompileTimeoutMs(InfiniteTimeoutMs);
        appConfig.MutableTableServiceConfig()->MutableQueryLimits()->SetDataQueryTimeoutMs(InfiniteTimeoutMs);
        appConfig.MutableFeatureFlags()->SetEnableStatistics(false);

        TKikimrRunner kikimr(TKikimrSettings(appConfig).SetWithSampleTables(false));
        MakeDirectory(kikimr, "/Root/pg-rbo");

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        CreateSchema(session);

        constexpr ui32 ExplainRuns = 1;
        const TString query = ReadFixture(QueryPath);
        const auto countersBefore = GetNewRboCompileCounters(kikimr);
        const auto explainSettings = TExplainDataQuerySettings()
            .ClientTimeout(infiniteTimeout)
            .OperationTimeout(infiniteTimeout)
            .CancelAfter(infiniteTimeout);

        for (ui32 i = 0; i < ExplainRuns; ++i) {
            auto result = session.ExplainDataQuery(query, explainSettings).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(!result.GetPlan().empty(), "Expected a non-empty explain plan");
        }

        const auto countersAfter = GetNewRboCompileCounters(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(countersAfter.first, countersBefore.first + ExplainRuns);
        UNIT_ASSERT_VALUES_EQUAL(countersAfter.second, countersBefore.second);
    }
}

} // namespace
} // namespace NKikimr::NKqp
