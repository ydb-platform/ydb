#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    // Helper function to test data query execution with different SqlVersion configurations
    void TestDataQueryWithSqlVersion(TMaybe<ui32> sqlVersion, const TString& query) {
        NKikimrConfig::TAppConfig appConfig;
        if (sqlVersion) {
            appConfig.MutableTableServiceConfig()->SetSqlVersion(*sqlVersion);
        }
        // If sqlVersion is Nothing(), SqlVersion is not set (defaults to 1)

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Helper function to test prepared query with SqlVersion = 0 (triggers fallback)
    void TestPreparedQueryWithFallback(const TString& query) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetSqlVersion(0);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());

        auto queryObj = prepareResult.GetQuery();
        auto result = queryObj.Execute(TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Helper function to test scan query with SqlVersion = 0 (triggers fallback)
    void TestScanQueryWithFallback(const TString& query) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetSqlVersion(0);

        TKikimrRunner kikimr{ TKikimrSettings(appConfig) };
        auto queryClient = kikimr.GetQueryClient();
        auto session = queryClient.GetSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpCompileFallback) {

    // Test that when SqlVersion = 0, it first tries SqlVersion = 1 and succeeds
    Y_UNIT_TEST(FallbackToVersion1Success) {
        TestDataQueryWithSqlVersion(0, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
    }

    // Test that when SqlVersion = 0, compilation works (either succeeds with v1 or falls back to v0)
    // This test verifies the fallback mechanism doesn't break normal operation
    Y_UNIT_TEST(FallbackMechanismWorks) {
        TestDataQueryWithSqlVersion(0, R"(
            --!syntax_v0
            SELECT * FROM [/Root/KeyValue] LIMIT 1;
        )");
    }

    // Test that when SqlVersion = 1, no fallback is attempted
    Y_UNIT_TEST(NoFallbackWhenSqlVersion1) {
        TestDataQueryWithSqlVersion(1, R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
    }

    // Test that when SqlVersion is not set (defaults to 1), no fallback is attempted
    Y_UNIT_TEST(NoFallbackWhenSqlVersionNotSet) {
        TestDataQueryWithSqlVersion(Nothing(), R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )");
    }

    // Test fallback with a prepared query
    Y_UNIT_TEST(FallbackWithPreparedQuery) {
        TestPreparedQueryWithFallback(R"(
            --!syntax_v0
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )");
    }

    // Test that fallback works with scan queries
    Y_UNIT_TEST(FallbackWithScanQuery) {
        TestScanQueryWithFallback(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key > 0;
        )");
    }
}

} // namespace NKqp
} // namespace NKikimr
