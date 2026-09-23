#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTablePathPrefixScopeRollout) {
    Y_UNIT_TEST(SameSessionAndCachedQueryFollowFlagUpdates) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.GetAppData().FeatureFlags.SetEnableSequentialTablePathPrefix(false);
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto scheme = kikimr.GetSchemeClient();
        for (const TString& path : {TString("/Root/first"), TString("/Root/second")}) {
            const auto result = scheme.MakeDirectory(path).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const auto create = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/first/users` (id Uint64 NOT NULL, PRIMARY KEY (id));
            CREATE TABLE `/Root/second/users` (id Uint64 NOT NULL, PRIMARY KEY (id));
        )").ExtractValueSync();
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto write = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/first/users` (id) VALUES (11u);
            UPSERT INTO `/Root/second/users` (id) VALUES (22u);
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());
        NYdb::NScripting::TScriptingClient scripting(kikimr.GetDriver());
        const TString query = R"(
            PRAGMA TablePathPrefix = '/Root/first';
            SELECT id FROM users;
            PRAGMA TablePathPrefix = '/Root/second';
            SELECT id FROM users;
        )";
        const auto check = [](const auto& result, bool enabled) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            CompareYson(enabled ? "[[11u]]" : "[[22u]]", FormatResultSetYson(result.GetResultSets()[0]));
            CompareYson("[[22u]]", FormatResultSetYson(result.GetResultSets()[1]));
        };
        for (bool enabled : {false, true, false}) {
            runtime.GetAppData().FeatureFlags.SetEnableSequentialTablePathPrefix(enabled);
            for (unsigned repeat = 0; repeat < 2; ++repeat) {
                const auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                    TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
                check(result, enabled);
                UNIT_ASSERT(result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(TProtoAccessor::GetProto(*result.GetStats()).compilation().from_cache(), repeat != 0);
            }
            check(scripting.ExecuteYqlScript(query).ExtractValueSync(), enabled);
        }
    }
}

} // namespace NKikimr::NKqp
