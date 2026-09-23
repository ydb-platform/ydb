#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTablePathPrefixRelativeRollout) {
    Y_UNIT_TEST(SameSessionAndCachedQueryFollowFlagUpdates) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.GetAppData().FeatureFlags.SetEnableTablePathPrefixRelativePaths(false);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = kikimr.GetSchemeClient();
        for (const TString& path : {TString("/Root/folder"), TString("/Root/Root"), TString("/Root/Root/folder")}) {
            const auto result = scheme.MakeDirectory(path).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const auto create = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/folder/users` (id Uint64 NOT NULL, PRIMARY KEY (id));
            CREATE TABLE `/Root/Root/folder/users` (id Uint64 NOT NULL, PRIMARY KEY (id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto write = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/folder/users` (id) VALUES (11u);
            UPSERT INTO `/Root/Root/folder/users` (id) VALUES (22u);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        const TString pragma = "PRAGMA TablePathPrefix = './Root/folder';";
        for (bool enabled : {false, true, false}) {
            runtime.GetAppData().FeatureFlags.SetEnableTablePathPrefixRelativePaths(enabled);
            for (unsigned repeat = 0; repeat < 2; ++repeat) {
                const auto result = session.ExecuteDataQuery(pragma + "SELECT id FROM users;",
                    TTxControl::BeginTx().CommitTx(), TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                UNIT_ASSERT(result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache(), repeat != 0);
                CompareYson(enabled ? "[[22u]]" : "[[11u]]", FormatResultSetYson(result.GetResultSet(0)));
            }

            // ExecuteSchemeQuery reuses the worker created before the flag update.
            const auto ddl = session.ExecuteSchemeQuery(pragma +
                "CREATE TABLE probe (id Uint64, PRIMARY KEY (id));").ExtractValueSync();
            UNIT_ASSERT_C(ddl.IsSuccess(), ddl.GetIssues().ToString());
            const TString expected = enabled ? "/Root/Root/folder/probe" : "/Root/folder/probe";
            const auto description = session.DescribeTable(expected).ExtractValueSync();
            UNIT_ASSERT_C(description.IsSuccess(), expected << ": " << description.GetIssues().ToString());
            const auto drop = session.ExecuteSchemeQuery(pragma + "DROP TABLE probe;").ExtractValueSync();
            UNIT_ASSERT_C(drop.IsSuccess(), drop.GetIssues().ToString());
        }
    }
}

} // namespace NKikimr::NKqp
