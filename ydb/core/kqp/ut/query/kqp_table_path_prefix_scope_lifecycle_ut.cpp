#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/kqp/common/compilation/events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpTablePathPrefixScopeLifecycle) {
    Y_UNIT_TEST_TWIN(InFlightCompilationDoesNotServeRequestsAfterFlagUpdate, AstCache) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableTablePathPrefixMultiScopes(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableAstCache(AstCache);
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        auto client = kikimr.GetTableClient();
        auto oldSession = kikimr.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });
        auto newSession = kikimr.RunCall([&] { return client.CreateSession().GetValueSync().GetSession(); });

        const auto directory = kikimr.RunCall([&] {
            return kikimr.GetSchemeClient().MakeDirectory("/Root/Root").ExtractValueSync();
        });
        UNIT_ASSERT_C(directory.IsSuccess(), directory.GetIssues().ToString());
        const auto create = kikimr.RunCall([&] {
            return oldSession.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/items` (id Uint64 NOT NULL, PRIMARY KEY (id));
                CREATE TABLE `/Root/Root/items` (id Uint64 NOT NULL, PRIMARY KEY (id));
            )").ExtractValueSync();
        });
        UNIT_ASSERT_C(create.IsSuccess(), create.GetIssues().ToString());
        const auto write = kikimr.RunCall([&] {
            return oldSession.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/items` (id) VALUES (1u);
                UPSERT INTO `/Root/Root/items` (id) VALUES (2u);
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        });
        UNIT_ASSERT_C(write.IsSuccess(), write.GetIssues().ToString());

        const TString query = R"(
            PRAGMA TablePathPrefix = '/Root';
            SELECT id FROM items;
            PRAGMA TablePathPrefix = '/Root/Root';
            SELECT id FROM items;
        )";
        const auto execute = [&](TSession& session) {
            return session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic))
                .ExtractValueSync();
        };

        // Hold the actor's first completed compilation before the service can
        // insert it into the cache or share it with queued requests.
        NActors::TBlockEvents<TEvKqp::TEvCompileResponse> oldCompilation(runtime, [&](const auto& ev) {
            const auto& result = ev->Get()->CompileResult;
            return result && result->Query && result->Query->Text == query;
        });
        auto oldFuture = kikimr.RunInThreadPool([&] { return execute(oldSession); });
        runtime.WaitFor("old TablePathPrefix compilation", [&] {
            return !oldCompilation.empty() || oldFuture.HasValue();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(!oldCompilation.empty(), "Query must reach compilation before the flag update");
        UNIT_ASSERT_VALUES_EQUAL(oldCompilation.front()->Get()->CompileResult->Status, Ydb::StatusIds::SUCCESS);

        runtime.GetAppData().FeatureFlags.SetEnableTablePathPrefixMultiScopes(true);
        TKqpCounters counters(runtime.GetAppData().Counters);
        auto newFuture = kikimr.RunInThreadPool([&] { return execute(newSession); });
        runtime.WaitFor("new request queued behind old compilation", [&] {
            return counters.CompileQueueSize->Val() != 0 || newFuture.HasValue();
        }, TDuration::Seconds(30));
        UNIT_ASSERT_C(counters.CompileQueueSize->Val() != 0, "New request must wait for the active compilation");
        oldCompilation.Stop().Unblock();

        const auto oldResult = runtime.WaitFuture(oldFuture);
        UNIT_ASSERT_C(oldResult.IsSuccess(), oldResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(oldResult.GetResultSets().size(), 2);
        CompareYson("[[2u]]", FormatResultSetYson(oldResult.GetResultSet(0)));
        CompareYson("[[2u]]", FormatResultSetYson(oldResult.GetResultSet(1)));
        const auto newResult = runtime.WaitFuture(newFuture);
        UNIT_ASSERT_C(newResult.IsSuccess(), newResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(newResult.GetResultSets().size(), 2);
        CompareYson("[[1u]]", FormatResultSetYson(newResult.GetResultSet(0)));
        CompareYson("[[2u]]", FormatResultSetYson(newResult.GetResultSet(1)));
        UNIT_ASSERT(newResult.GetStats());
        UNIT_ASSERT(!TProtoAccessor::GetProto(*newResult.GetStats()).compilation().from_cache());

        const auto cachedResult = kikimr.RunCall([&] { return execute(newSession); });
        UNIT_ASSERT_C(cachedResult.IsSuccess(), cachedResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(cachedResult.GetResultSets().size(), 2);
        CompareYson("[[1u]]", FormatResultSetYson(cachedResult.GetResultSet(0)));
        CompareYson("[[2u]]", FormatResultSetYson(cachedResult.GetResultSet(1)));
        UNIT_ASSERT(cachedResult.GetStats());
        UNIT_ASSERT(TProtoAccessor::GetProto(*cachedResult.GetStats()).compilation().from_cache());
    }
}

} // namespace NKikimr::NKqp
