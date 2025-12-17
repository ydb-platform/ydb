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

void FillCache(TKikimrRunner& kikimr, ui32 queryCount) {
    auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
    auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
    
    for (ui32 i = 0; i < queryCount; ++i) {
        TString query = TStringBuilder() 
            << "SELECT Key, Value FROM `/Root/KeyValue` WHERE Key = " << i << ";";
        
        TExecDataQuerySettings settings;
        settings.KeepInQueryCache(true);
        
        auto result = kikimr.RunCall([&] { 
            return session.ExecuteDataQuery(
                query, 
                TTxControl::BeginTx().CommitTx(),
                settings
            ).ExtractValueSync(); 
        });
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    }
    

}

ui64 GetCompileCacheCount(TKikimrRunner& kikimr) {
    auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
    auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
    
    auto result = kikimr.RunCall([&] {
        return session.ExecuteDataQuery(
            "SELECT COUNT(*) AS cnt FROM `/Root/.sys/compile_cache_queries`",
            TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();
    });
    
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
    
    auto resultSet = result.GetResultSet(0);
    TResultSetParser parser(resultSet);
    UNIT_ASSERT(parser.TryNextRow());
    return parser.ColumnParser("cnt").GetUint64();
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWarmup) {

    Y_UNIT_TEST(WarmupActorBasic) {
        ui32 nodeCount = 2;
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        settings.SetNodeCount(nodeCount);

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableCompileCacheView(true);
        settings.SetFeatureFlags(featureFlags);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        FillCache(kikimr, 10);

        ui64 cacheCountBeforeWarmup = GetCompileCacheCount(kikimr);
        UNIT_ASSERT_C(cacheCountBeforeWarmup > 0,
            "Compile cache should have entries after executing queries, got: " << cacheCountBeforeWarmup);
        Cerr << "=== Cache count before warmup: " << cacheCountBeforeWarmup << Endl;

        TKqpWarmupConfig warmupActorConfig;
        warmupActorConfig.Enabled = true;
        warmupActorConfig.Deadline = TDuration::Seconds(30);
        warmupActorConfig.CompileCacheWarmupEnabled = true;
        warmupActorConfig.MaxConcurrentCompilations = 5;

        ui32 const nodeId = 0;
        auto warmupEdge = runtime.AllocateEdgeActor(nodeId);
        auto* warmupActor = CreateKqpWarmupActor(warmupActorConfig, warmupEdge, "/Root", "");
        runtime.Register(warmupActor, nodeId);

        auto warmupComplete = runtime.GrabEdgeEvent<TEvKqpWarmupComplete>(warmupEdge, TDuration::Seconds(30));

        UNIT_ASSERT_C(warmupComplete, "Warmup actor did not complete within timeout");
        UNIT_ASSERT_C(warmupComplete->Get()->Success, 
            "Warmup should complete successfully: " << warmupComplete->Get()->Message);
        UNIT_ASSERT_C(warmupComplete->Get()->EntriesLoaded > cacheCountBeforeWarmup * nodeCount, 
            "Entries loaded: " << warmupComplete->Get()->EntriesLoaded << " != " << cacheCountBeforeWarmup);
    }
}

} // namespace NKikimr::NKqp

