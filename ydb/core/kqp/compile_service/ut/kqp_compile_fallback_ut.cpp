#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    void EnableCompileDebugLogging(TKikimrRunner& kikimr) {
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::KQP_COMPILE_ACTOR, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NLog::PRI_DEBUG);
    }

    std::pair<ui32, ui32> GetEnforceConfigCounters(TKikimrRunner& kikimr) {
        auto counters = TKqpCounters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        return {
            counters.GetKqpCounters()->GetCounter("Compilation/EnforceConfig/Success")->Val(),
            counters.GetKqpCounters()->GetCounter("Compilation/EnforceConfig/Failed")->Val()
        };
    }

    std::pair<ui32, ui32> TestDataQueryWithSqlVersion(TMaybe<ui32> sqlVersion, const TString& query, bool enforceSqlVersionV1 = true) {
        NKikimrConfig::TAppConfig appConfig;
        if (sqlVersion) {
            appConfig.MutableTableServiceConfig()->SetSqlVersion(*sqlVersion);
        }

        appConfig.MutableTableServiceConfig()->SetEnforceSqlVersionV1(enforceSqlVersionV1);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        EnableCompileDebugLogging(kikimr);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        return GetEnforceConfigCounters(kikimr);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpCompileFallback) {

    Y_UNIT_TEST(CompileWithSqlVersionZeroUsesV1) {
        auto [success, failed] = TestDataQueryWithSqlVersion(0, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        UNIT_ASSERT(success > 0);
        UNIT_ASSERT_VALUES_EQUAL(failed, 0);
    }

    Y_UNIT_TEST(NoFallbackWhenSqlVersion1) {
        auto [success, failed] = TestDataQueryWithSqlVersion(1, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        UNIT_ASSERT(success == 0);
        UNIT_ASSERT(failed == 0);
    }

    Y_UNIT_TEST(NoFallbackWhenSqlVersionNotSet) {
        auto [success, failed] = TestDataQueryWithSqlVersion(Nothing(), R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        UNIT_ASSERT(success == 0);
        UNIT_ASSERT(failed == 0);
    }

    Y_UNIT_TEST(V0SyntaxQueryFailsWithoutFallback) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetSqlVersion(0);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        EnableCompileDebugLogging(kikimr);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());

        auto [success, failed] = GetEnforceConfigCounters(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(failed, 0);
        UNIT_ASSERT_VALUES_EQUAL(success, 0);
    }
}

} // namespace NKqp
} // namespace NKikimr
