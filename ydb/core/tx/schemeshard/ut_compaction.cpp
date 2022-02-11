#include "operation_queue_timer.h"

#include <ydb/core/cms/console/console.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <algorithm>
#include <random>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

using TTableInfoMap = THashMap<TString, NKikimrTxDataShard::TEvGetInfoResponse::TUserTable>;

std::pair<TTableInfoMap, ui64> GetTables(
    TTestActorRuntime &runtime,
    ui64 tabletId)
{
    auto sender = runtime.AllocateEdgeActor();
    auto request = MakeHolder<TEvDataShard::TEvGetInfoRequest>();
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TTableInfoMap result;

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetInfoResponse>(handle);
    for (auto& table: response->Record.GetUserTables()) {
        result[table.GetName()] = table;
    }

    auto ownerId = response->Record.GetTabletInfo().GetSchemeShard();

    return std::make_pair(result, ownerId);
}

void SetFeatures(
    TTestActorRuntime &runtime,
    TTestEnv&,
    ui64 schemeShard,
    const NKikimrConfig::TFeatureFlags& features)
{
    auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    *request->Record.MutableConfig()->MutableFeatureFlags() = features;

    // little hack to simplify life
    auto* compactionConfig = request->Record.MutableConfig()->MutableCompactionConfig();
    compactionConfig->MutableBackgroundCompactionConfig()->SetSearchHeightThreshold(0);
    compactionConfig->MutableBackgroundCompactionConfig()->SetRowCountThreshold(0);

    auto sender = runtime.AllocateEdgeActor();

    runtime.SendToPipe(schemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(handle);
}

void SetBackgroundCompactionServerless(TTestActorRuntime &runtime, TTestEnv& env, ui64 schemeShard, bool value) {
    NKikimrConfig::TFeatureFlags features;
    features.SetEnableBackgroundCompactionServerless(value);
    SetFeatures(runtime, env, schemeShard, features);
}

void SetBackgroundCompaction(TTestActorRuntime &runtime, TTestEnv& env, ui64 schemeShard, bool value) {
    NKikimrConfig::TFeatureFlags features;
    features.SetEnableBackgroundCompaction(value);
    SetFeatures(runtime, env, schemeShard, features);
}

void DisableBackgroundCompactionViaRestart(
    TTestActorRuntime& runtime,
    TTestEnv&,
    ui64 schemeShard)
{
    // turn on background compaction and restart to apply
    runtime.GetAppData().FeatureFlags.SetEnableBackgroundCompactionForTest(false);
    runtime.GetAppData().FeatureFlags.SetEnableBackgroundCompactionServerlessForTest(false);

    // little hack to simplify life
    auto& compactionConfig = runtime.GetAppData().CompactionConfig;
    compactionConfig.MutableBackgroundCompactionConfig()->SetSearchHeightThreshold(0);
    compactionConfig.MutableBackgroundCompactionConfig()->SetRowCountThreshold(0);

    TActorId sender = runtime.AllocateEdgeActor();
    RebootTablet(runtime, schemeShard, sender);
}

void EnableBackgroundCompactionViaRestart(
    TTestActorRuntime& runtime,
    TTestEnv&,
    ui64 schemeShard,
    bool enableServerless)
{
    // turn on background compaction and restart to apply
    runtime.GetAppData().FeatureFlags.SetEnableBackgroundCompactionForTest(true);
    runtime.GetAppData().FeatureFlags.SetEnableBackgroundCompactionServerlessForTest(enableServerless);

    // little hack to simplify life
    auto& compactionConfig = runtime.GetAppData().CompactionConfig;
    compactionConfig.MutableBackgroundCompactionConfig()->SetSearchHeightThreshold(0);
    compactionConfig.MutableBackgroundCompactionConfig()->SetRowCountThreshold(0);

    TActorId sender = runtime.AllocateEdgeActor();
    RebootTablet(runtime, schemeShard, sender);
}

ui64 GetCompactionsCount(
    TTestActorRuntime &runtime,
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    ui64 tabletId,
    ui64 ownerId)
{
    auto sender = runtime.AllocateEdgeActor();

    auto request = MakeHolder<TEvDataShard::TEvGetCompactTableStats>(ownerId, userTable.GetPathId());
    runtime.SendToPipe(tabletId, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvGetCompactTableStatsResult>(handle);
    UNIT_ASSERT(response->Record.HasBackgroundCompactionRequests());

    return response->Record.GetBackgroundCompactionRequests();
}

ui64 GetCompactionsCount(
    TTestActorRuntime &runtime,
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TVector<ui64>& shards,
    ui64 ownerId)
{
    ui64 compactionsCount = 0;
    for (auto shard: shards) {
        compactionsCount += GetCompactionsCount(
            runtime,
            userTable,
            shard,
            ownerId);
    }

    return compactionsCount;
}

void CheckShardCompacted(
    TTestActorRuntime &runtime,
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    ui64 tabletId,
    ui64 ownerId,
    bool shouldCompacted = true)
{
    auto count = GetCompactionsCount(
        runtime,
        userTable,
        tabletId,
        ownerId);

    if (shouldCompacted) {
        UNIT_ASSERT(count > 0);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(count, 0UL);
    }
}

void CheckNoCompactions(
    TTestActorRuntime &runtime,
    TTestEnv& env,
    ui64 schemeshardId,
    const TString& path)
{
    auto description = DescribePrivatePath(runtime, schemeshardId, path, true, true);
    TVector<ui64> shards;
    for (auto &part : description.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    UNIT_ASSERT(!shards.empty());

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto [tables, ownerId] = GetTables(runtime, shards.at(0));

    auto userTableName = TStringBuf(path).RNextTok('/');
    const auto& userTable = tables[userTableName];

    auto count1 = GetCompactionsCount(
        runtime,
        userTable,
        shards,
        ownerId);

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto count2 = GetCompactionsCount(
        runtime,
        userTable,
        shards,
        ownerId);

    UNIT_ASSERT_VALUES_EQUAL(count1, count2);
}

template<typename F>
void TestBackgroundCompaction(
    TTestActorRuntime& runtime,
    TTestEnv& env,
    F&& enableBackgroundCompactionFunc)
{
    ui64 txId = 1000;

    TestCreateTable(runtime, ++txId, "/MyRoot",
        R"____(
            Name: "Simple"
            Columns { Name: "key1"  Type: "Uint32"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["key1"]
            UniformPartitionsCount: 2
        )____");
    env.TestWaitNotification(runtime, txId);

    enableBackgroundCompactionFunc(runtime, env);

    auto description = DescribePrivatePath(runtime, "/MyRoot/Simple", true, true);
    TVector<ui64> shards;
    for (auto &part : description.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto [tables, ownerId] = GetTables(runtime, shards.at(0));

    for (auto shard: shards)
        CheckShardCompacted(runtime, tables["Simple"], shard, ownerId);
}

ui64 TestServerless(
    TTestActorRuntime& runtime,
    TTestEnv& env,
    bool enableServerless)
{
    ui64 txId = 100;
    ui64 schemeshardId = TTestTxConfig::SchemeShard;

    TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", R"(
        Name: "Shared"
    )");
    env.TestWaitNotification(runtime, txId);

    TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", R"(
        PlanResolution: 50
        Coordinators: 1
        Mediators: 1
        TimeCastBucketsPerMediator: 2
        ExternalSchemeShard: true
        Name: "Shared"
        StoragePools {
            Name: "name_User_kind_hdd-1"
            Kind: "common"
        }
        StoragePools {
            Name: "name_User_kind_hdd-2"
            Kind: "external"
        }
    )");
    env.TestWaitNotification(runtime, txId);

    const auto attrs = AlterUserAttrs({
        {"cloud_id", "CLOUD_ID_VAL"},
        {"folder_id", "FOLDER_ID_VAL"},
        {"database_id", "DATABASE_ID_VAL"}
    });

    TestCreateExtSubDomain(runtime, ++txId, "/MyRoot", Sprintf(R"(
        Name: "User"
        ResourcesDomainKey {
            SchemeShard: %lu
            PathId: 2
        }
    )", schemeshardId), attrs);
    env.TestWaitNotification(runtime, txId);

    TestAlterExtSubDomain(runtime, ++txId, "/MyRoot", R"(
        PlanResolution: 50
        Coordinators: 1
        Mediators: 1
        TimeCastBucketsPerMediator: 2
        ExternalSchemeShard: true
        ExternalHive: false
        Name: "User"
        StoragePools {
            Name: "name_User_kind_hdd-1"
            Kind: "common"
        }
        StoragePools {
            Name: "name_User_kind_hdd-2"
            Kind: "external"
        }
    )");
    env.TestWaitNotification(runtime, txId);

    TestDescribeResult(DescribePath(runtime, "/MyRoot/User"), {
        NLs::PathExist,
        NLs::ExtractTenantSchemeshard(&schemeshardId)
    });

    TestCreateTable(runtime, schemeshardId, ++txId, "/MyRoot/User",
        R"____(
            Name: "Simple"
            Columns { Name: "key1"  Type: "Uint32"}
            Columns { Name: "Value" Type: "Utf8"}
            KeyColumnNames: ["key1"]
            UniformPartitionsCount: 2
        )____");
    env.TestWaitNotification(runtime, txId, schemeshardId);

    // turn on background compaction
    EnableBackgroundCompactionViaRestart(runtime, env, schemeshardId, enableServerless);

    auto description = DescribePrivatePath(runtime, schemeshardId, "/MyRoot/User/Simple", true, true);
    TVector<ui64> shards;
    for (auto &part : description.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto [tables, ownerId] = GetTables(runtime, shards.at(0));

    for (auto shard: shards)
        CheckShardCompacted(runtime, tables["Simple"], shard, ownerId, enableServerless);

    return schemeshardId;
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeshardBackgroundCompactionTest) {
    Y_UNIT_TEST(SchemeshardShouldRequestCompactionsSchemeshardRestart) {
        // enabled via schemeshard restart
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        // disable for the case, when compaction is enabled by default
        DisableBackgroundCompactionViaRestart(runtime, env, TTestTxConfig::SchemeShard);

        TestBackgroundCompaction(runtime, env, [](auto& runtime, auto& env) {
            EnableBackgroundCompactionViaRestart(runtime, env, TTestTxConfig::SchemeShard, false);
        });
    }

    Y_UNIT_TEST(SchemeshardShouldRequestCompactionsConfigRequest) {
        // enabled via configuration change
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        // disable for the case, when compaction is enabled by default
        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, false);

        TestBackgroundCompaction(runtime, env, [](auto& runtime, auto& env) {
            SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, true);
        });
    }

    Y_UNIT_TEST(SchemeshardShouldNotRequestCompactionsAfterDisable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        // disable for the case, when compaction is enabled by default
        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, false);

        TestBackgroundCompaction(runtime, env, [](auto& runtime, auto& env) {
            SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, true);
        });

        // disable
        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, false);

        // some time to finish compactions in progress
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        CheckNoCompactions(runtime, env, TTestTxConfig::SchemeShard, "/MyRoot/Simple");
    }

    Y_UNIT_TEST(ShouldNotCompactServerless) {
        // enable regular background compaction, but not serverless
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        TestServerless(runtime, env, false);
    }

    Y_UNIT_TEST(ShouldCompactServerless) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        TestServerless(runtime, env, true);
    }

    Y_UNIT_TEST(ShouldNotCompactServerlessAfterDisable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto schemeshardId = TestServerless(runtime, env, true);

        // disable
        SetBackgroundCompactionServerless(runtime, env, schemeshardId, false);

        // some time to finish compactions in progress
        env.SimulateSleep(runtime, TDuration::Seconds(30));

        CheckNoCompactions(runtime, env, schemeshardId, "/MyRoot/User/Simple");
    }
};

namespace NKikimr::NSchemeShard {

Y_UNIT_TEST_SUITE(TSchemeshardCompactionQueueTest) {
    constexpr TShardIdx ShardIdx = TShardIdx(11, 17);

    Y_UNIT_TEST(EnqueuBelowSearchHeightThreshold) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 3;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
    }

    Y_UNIT_TEST(EnqueueBelowRowDeletesThreshold) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 1;
        stats.SearchHeight = 20;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
    }

    Y_UNIT_TEST(ShouldNotEnqueueEmptyShard) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;
        config.RowCountThreshold = 1;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 0;
        stats.RowDeletes = 1;
        stats.SearchHeight = 20;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(!queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
    }

    Y_UNIT_TEST(UpdateBelowSearchHeightThreshold) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 20;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);

        stats.SearchHeight = 1;
        UNIT_ASSERT(queue.UpdateIfFound({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
    }

    Y_UNIT_TEST(UpdateBelowRowDeletesThreshold) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 1000;
        stats.SearchHeight = 20;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);

        stats.RowDeletes = 1;
        UNIT_ASSERT(queue.UpdateIfFound({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
    }

    Y_UNIT_TEST(UpdateWithEmptyShard) {
        TCompactionQueueImpl::TConfig config;
        config.RowCountThreshold = 1;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 1000;
        stats.SearchHeight = 20;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));

        stats.RowCount = 0;
        UNIT_ASSERT(queue.UpdateIfFound({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
    }

    Y_UNIT_TEST(ShouldPopWhenOnlyLastCompactionQueue) {
        TCompactionQueueImpl::TConfig config;
        config.RowCountThreshold = 0;
        config.SearchHeightThreshold = 100;
        config.RowDeletesThreshold = 100;

        auto makeInfo = [](ui64 idx, ui64 ts) {
            TShardIdx shardId = TShardIdx(1, idx);
            TTableInfo::TPartitionStats stats;
            stats.FullCompactionTs = ts;
            return TShardCompactionInfo(shardId, stats);
        };

        std::vector<TShardCompactionInfo> shardInfos = {
            //       id,   ts
            makeInfo(1,    1), 
            makeInfo(2,    2),
            makeInfo(3,    3),
            makeInfo(4,    4) 
        };

        auto rng = std::default_random_engine {};
        std::shuffle(shardInfos.begin(), shardInfos.end(), rng);

        TCompactionQueueImpl queue(config);
        for (const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);

        for (auto i: xrange(1ul, 5UL)) {
            UNIT_ASSERT(!queue.Empty());
            UNIT_ASSERT_VALUES_EQUAL(queue.Front().ShardIdx.GetLocalId().GetValue(), i);
            queue.PopFront();
        }

        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(CheckOrderWhenAllQueues) {
        TCompactionQueueImpl::TConfig config;
        config.RowCountThreshold = 0;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        auto makeInfo = [](ui64 idx, ui64 ts, ui64 sh, ui64 d) {
            TShardIdx shardId = TShardIdx(1, idx);
            TTableInfo::TPartitionStats stats;
            stats.FullCompactionTs = ts;
            stats.SearchHeight = sh;
            stats.RowDeletes = d;
            return TShardCompactionInfo(shardId, stats);
        };

        std::vector<TShardCompactionInfo> shardInfos = {
            //       id,   ts,     sh,     d
            makeInfo(1,    1,     100,    100),   // top in TS
            makeInfo(2,    3,     100,    50),    // top in SH
            makeInfo(3,    4,     50,     100),   // top in D
            makeInfo(4,    2,     0,      0),     // 2 in TS
            makeInfo(5,    3,     90,     0),     // 2 in SH
            makeInfo(6,    4,     0,      90),    // 2 in D
            makeInfo(7,    3,     0,      0),     // 3 in TS
            makeInfo(8,    5,     0,      80),    // 3 in D
            makeInfo(9,    5,     0,      0),     // 4 in TS, since this point only TS queue contains items
            makeInfo(10,   6,     0,      0),     // 5 in TS
            makeInfo(11,   7,     0,      0),     // 6 in TS
        };

        auto rng = std::default_random_engine {};
        std::shuffle(shardInfos.begin(), shardInfos.end(), rng);

        TCompactionQueueImpl queue(config);
        for(const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), shardInfos.size());
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 5UL);

        for (auto i: xrange(shardInfos.size())) {
            UNIT_ASSERT(!queue.Empty());
            UNIT_ASSERT_VALUES_EQUAL(queue.Front().ShardIdx.GetLocalId().GetValue(), i + 1);
            queue.PopFront();
        }

        UNIT_ASSERT(queue.Empty());
    }
};

} // NKikimr::NSchemeShard
