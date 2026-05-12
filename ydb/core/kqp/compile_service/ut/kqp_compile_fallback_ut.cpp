#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    // Helper function to enable debug logging for compile-related services
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

    // Helper function to test data query execution with different SqlVersion configurations
    // Returns counters for verification
    std::pair<ui32, ui32> TestDataQueryWithSqlVersion(TMaybe<ui32> sqlVersion, const TString& query, bool enforceSqlVersionV1 = true) {
        NKikimrConfig::TAppConfig appConfig;
        if (sqlVersion) {
            appConfig.MutableTableServiceConfig()->SetSqlVersion(*sqlVersion);
        }

        appConfig.MutableTableServiceConfig()->SetEnforceSqlVersionV1(enforceSqlVersionV1);
        // If sqlVersion is Nothing(), SqlVersion is not set (defaults to 1)

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        EnableCompileDebugLogging(kikimr);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        return GetEnforceConfigCounters(kikimr);
    }

    // Helper function to test prepared query with SqlVersion = 0 (triggers fallback)
    std::pair<ui32, ui32> TestPreparedQueryWithFallback(const TString& query) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetSqlVersion(0);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        EnableCompileDebugLogging(kikimr);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());

        auto queryObj = prepareResult.GetQuery();
        auto result = queryObj.Execute(TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return GetEnforceConfigCounters(kikimr);
    }

    // Helper function to test scan query with SqlVersion = 0 (triggers fallback)
    std::pair<ui32, ui32> TestScanQueryWithFallback(const TString& query) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetSqlVersion(0);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        EnableCompileDebugLogging(kikimr);

        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        return GetEnforceConfigCounters(kikimr);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpCompileFallback) {

    // Test that when SqlVersion = 0, it first tries SqlVersion = 1 and succeeds (no fallback)
    Y_UNIT_TEST(FallbackToVersion1Success) {
        auto [success, failed] = TestDataQueryWithSqlVersion(0, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        // Should succeed with SqlVersion = 1 on first try, so no fallback
        UNIT_ASSERT(success > 0);
        UNIT_ASSERT_VALUES_EQUAL(failed, 0);
    }

    // Test that when SqlVersion = 0, compilation works (either succeeds with v1 or falls back to v0)
    // This test verifies the fallback mechanism doesn't break normal operation
    Y_UNIT_TEST(FallbackMechanismWorks) {
        auto [success, failed]  = TestDataQueryWithSqlVersion(0, R"(
            SELECT * FROM [/Root/KeyValue] LIMIT 1;
        )");
        // Query should compile successfully (either with or without fallback)
        UNIT_ASSERT_VALUES_EQUAL(failed, 1);
    }

    // Test that when SqlVersion = 0, compilation works (either succeeds with v1 or falls back to v0) when EnforceSqlVersionV1 is false
    // This test verifies the fallback mechanism doesn't work when EnforceSqlVersionV1 is false
    Y_UNIT_TEST(FallbackMechanismWorksEnforceSqlVersionV1False) {
        auto [success, failed]  = TestDataQueryWithSqlVersion(0, R"(
            SELECT * FROM [/Root/KeyValue] LIMIT 1;
        )", false);
        // Query should compile successfully (either with or without fallback)
        UNIT_ASSERT_VALUES_EQUAL(failed, 0);
        UNIT_ASSERT_VALUES_EQUAL(success, 0);
    }

    // Test that when SqlVersion = 1, no fallback is attempted
    Y_UNIT_TEST(NoFallbackWhenSqlVersion1) {
        auto [success, failed] = TestDataQueryWithSqlVersion(1, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        // Should not use fallback when SqlVersion = 1
        UNIT_ASSERT(success == 0);
        UNIT_ASSERT(failed == 0);
    }

    // Test that when SqlVersion is not set (defaults to 1), no fallback is attempted
    Y_UNIT_TEST(NoFallbackWhenSqlVersionNotSet) {
        auto [success, failed] = TestDataQueryWithSqlVersion(Nothing(), R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
        // Should not use fallback when SqlVersion defaults to 1
        UNIT_ASSERT(success == 0);
        UNIT_ASSERT(failed == 0);
    }

    // Test fallback with a prepared query
    Y_UNIT_TEST(FallbackWithPreparedQuery) {
        auto [success, failed] = TestPreparedQueryWithFallback(R"(
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )");

        UNIT_ASSERT(failed > 0);
    }

    // Test that fallback works with scan queries
    Y_UNIT_TEST(FallbackWithScanQuery) {
        auto [success, failed] = TestScanQueryWithFallback(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key > 0;
        )");

        UNIT_ASSERT(success > 0);
    }
}

} // namespace NKqp
} // namespace NKikimr
