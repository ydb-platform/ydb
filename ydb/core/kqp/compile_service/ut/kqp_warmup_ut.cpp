#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/compile_service/kqp_warmup_actor.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void ExecuteVariousQueries(TKikimrRunner& kikimr, ui32 count) {
    auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
    auto session = kikimr.RunCall([&] { return db.GetSession().ExtractValueSync().GetSession(); });
    
    for (ui32 i = 0; i < count; ++i) {
        TString query = TStringBuilder() 
            << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << i << ";";
        
        auto result = kikimr.RunCall([&] { 
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync(); 
        });
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    }
}

ui64 GetCompileCacheCount(TKikimrRunner& kikimr) {
    auto db = kikimr.RunCall([&] { return kikimr.GetQueryClient(); });
    auto session = kikimr.RunCall([&] { return db.GetSession().ExtractValueSync().GetSession(); });
    
    auto result = kikimr.RunCall([&] {
        return session.ExecuteQuery(
            "SELECT COUNT(*) AS cnt FROM `/Root/.sys/compile_cache_queries`",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();
    });
    
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    
    auto resultSet = result.GetResultSetParser(0);
    UNIT_ASSERT(resultSet.TryNextRow());
    return resultSet.ColumnParser("cnt").GetUint64();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWarmup) {

    Y_UNIT_TEST(WarmupActorBasic) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(2);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        ExecuteVariousQueries(kikimr, 10);

        ui64 cacheCountBeforeWarmup = GetCompileCacheCount(kikimr);
        UNIT_ASSERT_C(cacheCountBeforeWarmup > 0,
            "Compile cache should have entries after executing queries, got: " << cacheCountBeforeWarmup);

        TKqpWarmupConfig warmupActorConfig;
        warmupActorConfig.Enabled = true;
        warmupActorConfig.Deadline = TDuration::Seconds(30);
        warmupActorConfig.CompileCacheWarmupEnabled = true;
        warmupActorConfig.MaxConcurrentCompilations = 5;

        auto warmupEdge = runtime.AllocateEdgeActor();
        auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, warmupEdge, "/Root");
        runtime.Register(warmupActor);

        auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, TDuration::Seconds(30));

        UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
        
        UNIT_ASSERT_C(warmupComplete->Get()->Success, 
            "Warmup should complete successfully: " << warmupComplete->Get()->Message);
        
        UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded == cacheCountBeforeWarmup, 
        "Entries loaded: " << warmupComplete->Get()->EntriesLoaded << " != " << cacheCountBeforeWarmup);
    }
}

} // namespace NKikimr::NKqp

