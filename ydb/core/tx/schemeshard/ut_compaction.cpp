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

TShardCompactionInfo MakeCompactionInfo(ui64 idx, ui64 ts, ui64 sh = 0, ui64 d = 0) {
    TShardIdx shardId = TShardIdx(1, idx);
    TTableInfo::TPartitionStats stats;
    stats.FullCompactionTs = ts;
    stats.SearchHeight = sh;
    stats.RowDeletes = d;
    stats.PartCount = 100; // random number to not consider shard as empty
    stats.RowCount = 100;  // random number to not consider shard as empty
    return TShardCompactionInfo(shardId, stats);
}

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

struct TPathInfo {
    ui64 OwnerId = TTestTxConfig::SchemeShard;
    NKikimrTxDataShard::TEvGetInfoResponse::TUserTable UserTable;
    TVector<ui64> Shards;
};

TPathInfo GetPathInfo(
    TTestActorRuntime &runtime,
    const char* fullPath,
    ui64 schemeshardId = TTestTxConfig::SchemeShard)
{
    TPathInfo info;
    auto description = DescribePrivatePath(runtime, schemeshardId,  fullPath, true, true);
    for (auto &part : description.GetPathDescription().GetTablePartitions())
        info.Shards.push_back(part.GetDatashardId());

    auto [tables, ownerId] = GetTables(runtime, info.Shards.at(0));
    auto userTableName = TStringBuf(fullPath).RNextTok('/');
    info.UserTable = tables[userTableName];
    info.OwnerId = ownerId;

    return info;
}

void CreateTableWithData(
    TTestActorRuntime &runtime,
    TTestEnv& env,
    const char* path,
    const char* name,
    ui32 shardsCount,
    ui64& txId,
    ui64 schemeshardId = TTestTxConfig::SchemeShard)
{
    TestCreateTable(runtime, schemeshardId, ++txId, path,
        Sprintf(R"____(
            Name: "%s"
            Columns { Name: "key"  Type: "Uint64"}
            Columns { Name: "value" Type: "Utf8"}
            KeyColumnNames: ["key"]
            UniformPartitionsCount: %d
        )____", name, shardsCount));
    env.TestWaitNotification(runtime, txId, schemeshardId);

    auto fnWriteRow = [&] (ui64 tabletId, ui64 key, const char* tableName) {
        TString writeQuery = Sprintf(R"(
            (
                (let key '( '('key (Uint64 '%lu)) ) )
                (let value '('('value (Utf8 'MostMeaninglessValueInTheWorld)) ) )
                (return (AsList (UpdateRow '__user__%s key value) ))
            )
        )", key, tableName);
        NKikimrMiniKQL::TResult result;
        TString err;
        NKikimrProto::EReplyStatus status = LocalMiniKQL(runtime, tabletId, writeQuery, result, err);
        UNIT_ASSERT_VALUES_EQUAL(err, "");
        UNIT_ASSERT_VALUES_EQUAL(status, NKikimrProto::EReplyStatus::OK);;
    };

    for (ui64 key = 0; key < 100; ++key) {
        fnWriteRow(TTestTxConfig::FakeHiveTablets, key, name);
    }
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
    compactionConfig->MutableBackgroundCompactionConfig()->SetCompactSinglePartedShards(true);

    // 1 compaction / second
    compactionConfig->MutableBackgroundCompactionConfig()->SetMinCompactionRepeatDelaySeconds(0);
    compactionConfig->MutableBackgroundCompactionConfig()->SetMaxRate(1);
    compactionConfig->MutableBackgroundCompactionConfig()->SetRoundSeconds(0);

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
    compactionConfig.MutableBackgroundCompactionConfig()->SetCompactSinglePartedShards(true);

    // 1 compaction / second
    compactionConfig.MutableBackgroundCompactionConfig()->SetMinCompactionRepeatDelaySeconds(0);
    compactionConfig.MutableBackgroundCompactionConfig()->SetMaxRate(1);
    compactionConfig.MutableBackgroundCompactionConfig()->SetRoundSeconds(0);

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
    compactionConfig.MutableBackgroundCompactionConfig()->SetCompactSinglePartedShards(true);

    // 1 compaction / second
    compactionConfig.MutableBackgroundCompactionConfig()->SetMinCompactionRepeatDelaySeconds(0);
    compactionConfig.MutableBackgroundCompactionConfig()->SetMaxRate(1);
    compactionConfig.MutableBackgroundCompactionConfig()->SetRoundSeconds(0);

    TActorId sender = runtime.AllocateEdgeActor();
    RebootTablet(runtime, schemeShard, sender);
}

struct TCompactionStats {
    ui64 BackgroundRequestCount = 0;
    ui64 BackgroundCompactionCount = 0;

    TCompactionStats() = default;

    TCompactionStats(const NKikimrTxDataShard::TEvGetCompactTableStatsResult& stats)
        : BackgroundRequestCount(stats.GetBackgroundCompactionRequests())
        , BackgroundCompactionCount(stats.GetBackgroundCompactionCount())
    {}

    void Update(const TCompactionStats& other) {
        BackgroundRequestCount += other.BackgroundRequestCount;
        BackgroundCompactionCount += other.BackgroundCompactionCount;
    }
};

TCompactionStats GetCompactionStats(
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

    return TCompactionStats(response->Record);
}

TCompactionStats GetCompactionStats(
    TTestActorRuntime &runtime,
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    const TVector<ui64>& shards,
    ui64 ownerId)
{
    TCompactionStats stats;

    for (auto shard: shards) {
        stats.Update(GetCompactionStats(
            runtime,
            userTable,
            shard,
            ownerId));
    }

    return stats;
}

TCompactionStats GetCompactionStats(
    TTestActorRuntime &runtime,
    const TString& path,
    ui64 schemeshardId = TTestTxConfig::SchemeShard)
{
    auto info = GetPathInfo(runtime, path.c_str(), schemeshardId);
    UNIT_ASSERT(!info.Shards.empty());

    return GetCompactionStats(
        runtime,
        info.UserTable,
        info.Shards,
        info.OwnerId);
}

void CheckShardCompacted(
    TTestActorRuntime &runtime,
    const NKikimrTxDataShard::TEvGetInfoResponse::TUserTable& userTable,
    ui64 tabletId,
    ui64 ownerId,
    bool shouldCompacted = true)
{
    auto count = GetCompactionStats(
        runtime,
        userTable,
        tabletId,
        ownerId).BackgroundRequestCount;

    if (shouldCompacted) {
        UNIT_ASSERT(count > 0);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(count, 0UL);
    }
}

void CheckNoCompactionsInPeriod(
    TTestActorRuntime &runtime,
    TTestEnv& env,
    const TString& path,
    ui64 schemeshardId = TTestTxConfig::SchemeShard)
{
    auto info = GetPathInfo(runtime, path.c_str(), schemeshardId);
    UNIT_ASSERT(!info.Shards.empty());

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto count1 = GetCompactionStats(
        runtime,
        info.UserTable,
        info.Shards,
        info.OwnerId).BackgroundRequestCount;

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    auto count2 = GetCompactionStats(
        runtime,
        info.UserTable,
        info.Shards,
        info.OwnerId).BackgroundRequestCount;

    UNIT_ASSERT_VALUES_EQUAL(count1, count2);
}

template<typename F>
void TestBackgroundCompaction(
    TTestActorRuntime& runtime,
    TTestEnv& env,
    F&& enableBackgroundCompactionFunc)
{
    ui64 txId = 1000;

    CreateTableWithData(runtime, env, "/MyRoot", "Simple", 2, txId);
    auto info = GetPathInfo(runtime, "/MyRoot/Simple");

    enableBackgroundCompactionFunc(runtime, env);
    env.SimulateSleep(runtime, TDuration::Seconds(30));

    for (auto shard: info.Shards)
        CheckShardCompacted(runtime, info.UserTable, shard, info.OwnerId);
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

    auto info = GetPathInfo(runtime, "/MyRoot/User/Simple", schemeshardId);
    UNIT_ASSERT(!info.Shards.empty());

    env.SimulateSleep(runtime, TDuration::Seconds(30));

    for (auto shard: info.Shards)
        CheckShardCompacted(runtime, info.UserTable, shard, info.OwnerId, enableServerless);

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

        CheckNoCompactionsInPeriod(runtime, env, "/MyRoot/Simple");
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

        CheckNoCompactionsInPeriod(runtime, env, "/MyRoot/User/Simple", schemeshardId);
    }

    Y_UNIT_TEST(SchemeshardShouldNotCompactBackups) {
        // enabled via schemeshard restart
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        // disable for the case, when compaction is enabled by default
        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, false);

        ui64 txId = 1000;

        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 2, txId);

        // backup table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Simple"
            IsBackup: true
        )");
        env.TestWaitNotification(runtime, txId);

        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, true);

        CheckNoCompactionsInPeriod(runtime, env, "/MyRoot/CopyTable");
        UNIT_ASSERT_VALUES_EQUAL(GetCompactionStats(runtime, "/MyRoot/CopyTable").BackgroundRequestCount, 0UL);
    }

    Y_UNIT_TEST(SchemeshardShouldNotCompactBorrowed) {
        // enabled via schemeshard restart
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, NActors::NLog::PRI_TRACE);

        // disable for the case, when compaction is enabled by default
        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, false);

        // capture original observer func by setting dummy one
        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvCompactBorrowed:
                // we should not compact borrowed to check that background compaction
                // will not compact shard with borrowed parts as well
                Y_UNUSED(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            default:
                return originalObserver(runtime, ev);
            }
        });

        ui64 txId = 1000;

        // note that we create 1-sharded table to avoid complications
        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 1, txId);

        // copy table
        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Simple"
        )");
        env.TestWaitNotification(runtime, txId);

        SetBackgroundCompaction(runtime, env, TTestTxConfig::SchemeShard, true);

        CheckNoCompactionsInPeriod(runtime, env, "/MyRoot/CopyTable");
        UNIT_ASSERT_VALUES_EQUAL(GetCompactionStats(runtime, "/MyRoot/CopyTable").BackgroundRequestCount, 0UL);

        // original table should not be compacted as well
        CheckNoCompactionsInPeriod(runtime, env, "/MyRoot/Simple");
    }
};

namespace NKikimr::NSchemeShard {

Y_UNIT_TEST_SUITE(TSchemeshardCompactionQueueTest) {
    constexpr TShardIdx ShardIdx = TShardIdx(11, 17);

    Y_UNIT_TEST(EnqueueEmptyShard) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 0;
        config.RowDeletesThreshold = 0;

        TTableInfo::TPartitionStats stats; // all zeros

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(!queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
    }

    Y_UNIT_TEST(EnqueueSinglePartedShard) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 0;
        config.RowDeletesThreshold = 0;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 1; // below threshold
        stats.PartCount = 1;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(!queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
    }

    Y_UNIT_TEST(EnqueueSinglePartedShardWhenEnabled) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 0;
        config.RowDeletesThreshold = 0;
        config.CompactSinglePartedShards = true; // turn on

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 1; // below threshold
        stats.PartCount = 1;

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
    }

    Y_UNIT_TEST(ShouldNotEnqueueSinglePartedShardWithMemData) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 0;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 1; // below threshold
        stats.PartCount = 1;
        stats.MemDataSize = 10; // should be ignored

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(!queue.Enqueue({ShardIdx, stats}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
    }

    Y_UNIT_TEST(EnqueuBelowSearchHeightThreshold) {
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        TTableInfo::TPartitionStats stats;
        stats.RowCount = 10;
        stats.RowDeletes = 100;
        stats.SearchHeight = 3;
        stats.PartCount = 100; // random number to not consider shard as empty

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
        stats.PartCount = 100; // random number to not consider shard as empty

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

    Y_UNIT_TEST(RemoveLastShardFromSubQueues) {
        // check that when last shard is removed from BySearchHeight
        // or from ByRowDeletes, active queue is properly switched
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        std::vector<TShardCompactionInfo> shardInfos = {
            //                id,   ts,     sh,     d
            MakeCompactionInfo(0,    0,     0,      0),
            MakeCompactionInfo(1,    1,     0,      0),
            MakeCompactionInfo(2,    2,     0,      0),
            MakeCompactionInfo(3,    3,     0,      0),
            MakeCompactionInfo(4,    4,     100,    0),
            MakeCompactionInfo(5,    5,     100,    0),
            MakeCompactionInfo(6,    6,     0,      100),
            MakeCompactionInfo(7,    7,     0,      100),
        };

        TCompactionQueueImpl queue(config);
        for(const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        // initial queue state
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 8UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 8);

        // remove from LastCompaction, active queue should not change
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 0), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 7UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 7);

        // pop from LastCompaction, BySearchHeight is active now
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 6UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::BySearchHeight);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 2);

        // remove1 from BySearchHeight (active queue should not change)
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 4), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::BySearchHeight);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // remove2 from BySearchHeight, ByRowDeletes is active now
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 5), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByRowDeletes);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 2);

        // remove1 from ByRowDeletes
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 6), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByRowDeletes);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // remove2 from ByRowDeletes
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 7), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 2);

        // remove1 from LastCompaction
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 2), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // remove2 from LastCompaction
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 3), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 0);

        // check case BySearchHeight -> ByLastCompaction, i.e. empty ByRowDeletes

        shardInfos = {
            //                id,   ts,     sh,     d
            MakeCompactionInfo(1,    1,     0,      0),
            MakeCompactionInfo(2,    2,     0,      0),
            MakeCompactionInfo(3,    3,     0,      0),
            MakeCompactionInfo(4,    4,     100,    0),
        };

        for(const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        // pop from LastCompaction, BySearchHeight not empty
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::BySearchHeight);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // remove from BySearchHeight
        UNIT_ASSERT(queue.Remove({TShardIdx(1, 4), TTableInfo::TPartitionStats()}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 2);

        // check case ByLastCompaction -> ByRowDeletes, i.e. BySearchHeight is empty

        while (!queue.Empty())
            queue.PopFront();

        shardInfos = {
            //                id,   ts,     sh,     d
            MakeCompactionInfo(1,    1,     0,      0),
            MakeCompactionInfo(2,    2,     0,      0),
            MakeCompactionInfo(3,    3,     0,      0),
            MakeCompactionInfo(4,    4,     0,    100),
        };

        for(const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        // pop from LastCompaction, BySearchHeight empty, ByRowDeletes is not empty
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByRowDeletes);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);
    }

    Y_UNIT_TEST(UpdateBelowThreshold) {
        // check that last shard is removed (via low threshold and stats update) from BySearchHeight queue
        // while ByRowDeletes queue is not empty, thus becomes active
        TCompactionQueueImpl::TConfig config;
        config.SearchHeightThreshold = 10;
        config.RowDeletesThreshold = 10;

        std::vector<TShardCompactionInfo> shardInfos = {
            //                id,   ts,     sh,     d
            MakeCompactionInfo(1,    1,     0,      0),
            MakeCompactionInfo(2,    2,     0,      0),
            MakeCompactionInfo(3,    3,     0,      0),
            MakeCompactionInfo(4,    4,     100,    0),
            MakeCompactionInfo(5,    5,     0,      100),
        };

        TCompactionQueueImpl queue(config);
        for(const auto& info: shardInfos) {
            UNIT_ASSERT(queue.Enqueue(info));
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 5UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 5);

        // change to BySearchHeight queue
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::BySearchHeight);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        TTableInfo::TPartitionStats statsBelow;
        statsBelow.RowDeletes = 100;
        statsBelow.FullCompactionTs = 4;
        statsBelow.SearchHeight = 1; // below threshold
        statsBelow.RowDeletes = 1;   // below threshold
        statsBelow.PartCount = 100;  // random number to not consider shard as empty
        statsBelow.RowCount = 100;   // random number to not consider shard as empty

        // remove from BySearchHeight by updating stats (note that shard remains in LastCompaction queue)
        UNIT_ASSERT(queue.UpdateIfFound({TShardIdx(1, 4), statsBelow}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByRowDeletes);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // remove from BySearchHeight by updating stats (note that shard remains in LastCompaction queue)
        statsBelow.FullCompactionTs = 5;
        UNIT_ASSERT(queue.UpdateIfFound({TShardIdx(1, 5), statsBelow}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 4);

        // Now check transition from BySearchHeight to LastCompaction, i.e. empty RowDeletes

        // step1: populate w with item
        TTableInfo::TPartitionStats statsSh;
        statsSh.FullCompactionTs = 4;
        statsSh.SearchHeight = 100; // above threshold
        statsSh.RowDeletes = 1;     // below threshold
        statsSh.PartCount = 100;    // random number to not consider shard as empty
        statsSh.RowCount = 100;     // random number to not consider shard as empty
        UNIT_ASSERT(queue.UpdateIfFound({TShardIdx(1, 4), statsSh}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 4UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 4);

        // step2: change to BySearchHeight queue
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::BySearchHeight);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);

        // step3: BySearchHeight -> LastCompaction
        UNIT_ASSERT(queue.UpdateIfFound({TShardIdx(1, 4), statsBelow}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 3);

        // check ByLastCompaction -> ByRowDeletes, i.e. empty BySearchHeight

        // step1: populate ByRowDeletes with item
        TTableInfo::TPartitionStats statsDel;
        statsDel.FullCompactionTs = 5;
        statsDel.SearchHeight = 1; // below threshold
        statsDel.RowDeletes = 100; // above threshold
        statsDel.PartCount = 100;  // random number to not consider shard as empty
        statsDel.RowCount = 100;   // random number to not consider shard as empty
        UNIT_ASSERT(queue.UpdateIfFound({TShardIdx(1, 5), statsDel}));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByLastCompaction);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 3);

        // step2: change to ByRowDeletes
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeBySearchHeight(), 0UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.SizeByRowDeletes(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueType(), TCompactionQueueImpl::EActiveQueue::ByRowDeletes);
        UNIT_ASSERT_VALUES_EQUAL(queue.ActiveQueueSize(), 1);
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
        stats.PartCount = 100; // random number to not consider shard as empty

        TCompactionQueueImpl queue(config);
        UNIT_ASSERT(queue.Enqueue({ShardIdx, stats}));

        stats.RowCount = 0;
        stats.PartCount = 0;
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

        std::vector<TShardCompactionInfo> shardInfos = {
            //                id,   ts
            MakeCompactionInfo(1,    1),
            MakeCompactionInfo(2,    2),
            MakeCompactionInfo(3,    3),
            MakeCompactionInfo(4,    4)
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

        std::vector<TShardCompactionInfo> shardInfos = {
            //                 id,   ts,     sh,     d
            MakeCompactionInfo(1,    1,     100,    100),   // top in TS
            MakeCompactionInfo(2,    3,     100,    50),    // top in SH
            MakeCompactionInfo(3,    4,     50,     100),   // top in D
            MakeCompactionInfo(4,    2,     0,      0),     // 2 in TS
            MakeCompactionInfo(5,    3,     90,     0),     // 2 in SH
            MakeCompactionInfo(6,    4,     0,      90),    // 2 in D
            MakeCompactionInfo(7,    3,     0,      0),     // 3 in TS
            MakeCompactionInfo(8,    5,     0,      80),    // 3 in D
            MakeCompactionInfo(9,    5,     0,      0),     // 4 in TS, since this point only TS queue contains items
            MakeCompactionInfo(10,   6,     0,      0),     // 5 in TS
            MakeCompactionInfo(11,   7,     0,      0),     // 6 in TS
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
