#include <ydb/core/cms/console/console.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/protos/table_stats.pb.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

constexpr ui64 INITIAL_ROWS_COUNT = 100;

void WriteData(
    TTestActorRuntime &runtime,
    const char* name,
    ui64 fromKeyInclusive,
    ui64 toKey,
    ui64 tabletId = TTestTxConfig::FakeHiveTablets)
{
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

    for (ui64 key = fromKeyInclusive; key < toKey; ++key) {
        fnWriteRow(tabletId, key, name);
    }
}

void CreateTable(
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
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: %d
                    MaxPartitionsCount: %d
                }
            }
        )____", name, shardsCount, shardsCount, shardsCount));
    env.TestWaitNotification(runtime, txId, schemeshardId);
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
    CreateTable(runtime, env, path, name, shardsCount, txId, schemeshardId);
    WriteData(runtime, name, 0, INITIAL_ROWS_COUNT);
}

void WaitStat(
    TTestActorRuntime &runtime,
    TTestEnv& env,
    ui64 rowsExpected,
    ui64& storageStat)
{
    while (true) {
        auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple", true, true);
        ui64 rowCount = description.GetPathDescription().GetTableStats().GetRowCount();
        storageStat = description.GetPathDescription().GetTabletMetrics().GetStorage();
        if (rowCount == rowsExpected)
            break;
        env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
    }
}

void WaitAndCheckStatPersisted(
    TTestActorRuntime &runtime,
    TTestEnv& env,
    const ui64 rowsExpected,
    TDuration batchTimeout,
    TTestActorRuntime::EEventAction& eventAction,
    bool rowsShouldRestore = true)
{
    ui64 storageStatExpected = 0;
    WaitStat(runtime, env, rowsExpected, storageStatExpected);

    env.SimulateSleep(runtime, batchTimeout + TDuration::Seconds(1));

    // drop any further stat updates and restart SS
    // the only way for SS to know proper stat is to read it from localDB
    eventAction = TTestActorRuntime::EEventAction::DROP;

    TActorId sender = runtime.AllocateEdgeActor();
    GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

    auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple", true, true);
    ui64 rowCount = description.GetPathDescription().GetTableStats().GetRowCount();

    if (rowsShouldRestore)
        UNIT_ASSERT_VALUES_EQUAL(rowCount, rowsExpected);
    else
        UNIT_ASSERT_VALUES_EQUAL(rowCount, 0UL);

    // restore
    eventAction = TTestActorRuntime::EEventAction::PROCESS;
}

void SetStatsObserver(TTestActorRuntime& runtime, const std::function<TTestActorRuntime::EEventAction()>& statsObserver) {
    // capture original observer func by setting a dummy one
    auto originalObserver = runtime.SetObserverFunc([](TAutoPtr<IEventHandle>&) {
        return TTestActorRuntime::EEventAction::PROCESS;
    });
    // now set a custom observer backed up by the original
    runtime.SetObserverFunc([originalObserver, statsObserver](TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
        case TEvDataShard::EvPeriodicTableStats:
            return statsObserver();
        default:
            return originalObserver(ev);
        }
    });
}

TVector<ui64> GetTableShards(TTestActorRuntime& runtime,
                             const TString& path
) {
    TVector<ui64> shards;
    auto tableDescription = DescribePath(runtime, path, true);
    for (const auto& part : tableDescription.GetPathDescription().GetTablePartitions()) {
        shards.emplace_back(part.GetDatashardId());
    }

    return shards;
}

TTableId ResolveTableId(TTestActorRuntime& runtime, const TString& path) {
    auto response = Navigate(runtime, path);
    return response->ResultSet.at(0).TableId;
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeshardStatsBatchingTest) {
    Y_UNIT_TEST(ShouldNotBatchWhenDisabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);

        auto& appData = runtime.GetAppData();

        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto eventAction = TTestActorRuntime::EEventAction::PROCESS;
        SetStatsObserver(runtime, [&]() {
                return eventAction;
            }
        );

        ui64 txId = 1000;

        // note that we create 1-sharded table to avoid complications
        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 1, txId);

        WaitAndCheckStatPersisted(runtime, env, INITIAL_ROWS_COUNT, TDuration::Zero(), eventAction);
    }

    Y_UNIT_TEST(ShouldPersistByBatchSize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);
        const ui32 batchSize = 2;

        auto& appData = runtime.GetAppData();

        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);

        // set batching in a way it will finish only by batch size
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(10000000);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(batchSize);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto eventAction = TTestActorRuntime::EEventAction::PROCESS;
        ui64 statsCount = 0;
        SetStatsObserver(runtime, [&]() {
                ++statsCount;
                return eventAction;
            }
        );

        ui64 txId = 1000;

        // note that we create 1-sharded table to avoid complications
        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 1, txId);

        auto statsCountBefore = statsCount;
        eventAction = TTestActorRuntime::EEventAction::PROCESS;

        // now force split, when SS receives all stats it will finish its batch

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
                        Name: "Simple"
                        PartitionConfig {
                            PartitioningPolicy {
                                MinPartitionsCount: 20
                                MaxPartitionsCount: 20
                                SizeToSplit: 1
                            }
                        })");
        env.TestWaitNotification(runtime, txId);

        // we need this to fullfil batch so that actual split happens
        CreateTable(runtime, env, "/MyRoot", "Simple2", 1, txId);

        while (statsCount <= statsCountBefore + batchSize) {
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }

        WaitAndCheckStatPersisted(runtime, env, INITIAL_ROWS_COUNT, TDuration::Zero(), eventAction);
    }

    Y_UNIT_TEST(ShouldPersistByBatchTimeout) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(1);
        TDuration dsWakeupInterval = TDuration::Seconds(5); // hardcoded in DS
        TDuration batchTimeout = dsWakeupInterval;

        auto& appData = runtime.GetAppData();

        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);

        // set batching only by timeout
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(batchTimeout.MilliSeconds());
        appData.SchemeShardConfig.SetStatsMaxBatchSize(10000);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto eventAction = TTestActorRuntime::EEventAction::PROCESS;
        SetStatsObserver(runtime, [&]() {
                return eventAction;
            }
        );

        ui64 txId = 1000;

        // note that we create 1-sharded table to avoid complications
        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 1, txId);

        WaitAndCheckStatPersisted(runtime, env, INITIAL_ROWS_COUNT, batchTimeout, eventAction);

        // write more and check if timeout happens second time
        ui64 newRowsCount = INITIAL_ROWS_COUNT + 100;
        WriteData(runtime, "Simple", INITIAL_ROWS_COUNT, newRowsCount);

        WaitAndCheckStatPersisted(runtime, env, newRowsCount, batchTimeout, eventAction);
    }

    Y_UNIT_TEST(TopicAccountSizeAndUsedReserveSize) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NActors::NLog::PRI_TRACE);

        auto& appData = runtime.GetAppData();

        ui64 txId = 100;

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        const auto Assert = [&] (ui64 expectedAccountSize, ui64 expectedUsedReserveSize) {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Topic1"),
                               {NLs::Finished,
                                NLs::TopicAccountSize(expectedAccountSize),
                                NLs::TopicUsedReserveSize(expectedUsedReserveSize)});
        };

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 13
                    WriteSpeedInBytesPerSecond : 19
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(1 * 13 * 19, 0); // 247, 0

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic2"
            TotalGroupCount: 3
            PartitionPerTablet: 3
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 11
                    WriteSpeedInBytesPerSecond : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(1 * 13 * 19 + 3 * 11 * 17, 0); // 247 + 561 = 808, 0

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic3"
            TotalGroupCount: 3
            PartitionPerTablet: 3
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 11
                    WriteSpeedInBytesPerSecond : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(1 * 13 * 19 + 3 * 11 * 17 + 3 * 11 * 17, 0); // 247 + 561 + 561 = 1369, 0

        ui64 topic1Id = DescribePath(runtime, "/MyRoot/Topic1").GetPathDescription().GetSelf().GetPathId();
        ui64 topic2Id = DescribePath(runtime, "/MyRoot/Topic2").GetPathDescription().GetSelf().GetPathId();
        ui64 topic3Id = DescribePath(runtime, "/MyRoot/Topic3").GetPathDescription().GetSelf().GetPathId();

        ui64 generation = 1;
        ui64 round = 1;

        SendTEvPeriodicTopicStats(runtime, topic1Id, generation, ++round, 101, 101);
        Assert(1369, 101); // only reserve size

        SendTEvPeriodicTopicStats(runtime, topic1Id, generation, ++round, 383, 247);
        Assert(1369 + (383 - 247), 247); // 1505, 247 reserve + exceeding the limit

        SendTEvPeriodicTopicStats(runtime, topic2Id, generation, ++round, 113, 113);
        Assert(1369 + (383 - 247), 247 + 113); // 1505, 360

        SendTEvPeriodicTopicStats(runtime, topic1Id, generation, ++round, 31, 31);
        Assert(1369, 31 + 113); // only reserve, data size

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Topic2");
        env.TestWaitNotification(runtime, txId);
        Assert(808, 31);

        SendTEvPeriodicTopicStats(runtime, topic3Id, generation, ++round, 151, 151);
        Assert(808, 31 + 151);
    }

    Y_UNIT_TEST(TopicPeriodicStatMeteringModeReserved) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SimulateSleep(TDuration::MilliSeconds(128));

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NLog::PRI_TRACE);

        auto& appData = runtime.GetAppData();

        ui64 txId = 100;

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        appData.PQConfig.SetBalancerWakeupIntervalSec(1);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TString topicPath = "/MyRoot/Topic1";

        const auto Assert = [&] (ui64 expectedAccountSize, ui64 expectedUsedReserveSize) {
            TestDescribeResult(DescribePath(runtime,topicPath),
                               {NLs::Finished,
                                NLs::TopicAccountSize(expectedAccountSize),
                                NLs::TopicUsedReserveSize(expectedUsedReserveSize)});
        };


        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 3
            PartitionPerTablet: 3
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 2678400
                    WriteSpeedInBytesPerSecond : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(3 * 2678400 * 17, 0); // 136598400, 0

        auto msg = TString(24_MB, '_');

        ui32 msgSeqNo = 100;
        WriteToTopic(runtime, topicPath, msgSeqNo, msg);

        env.SimulateSleep(runtime, TDuration::Seconds(3)); // Wait TEvPeriodicTopicStats

        Assert(3 * 2678400 * 17, 16975298); // 16975298 - it is unstable value. it can change if internal message store change
    }

    Y_UNIT_TEST(TopicPeriodicStatMeteringModeRequest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SimulateSleep(TDuration::MilliSeconds(128));

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NLog::PRI_TRACE);

        auto& appData = runtime.GetAppData();

        ui64 txId = 100;

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        appData.PQConfig.SetBalancerWakeupIntervalSec(1);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        TString topicPath = "/MyRoot/Topic1";

        const auto Assert = [&] (ui64 expectedAccountSize, ui64 expectedUsedReserveSize) {
            TestDescribeResult(DescribePath(runtime,topicPath),
                               {NLs::Finished,
                                NLs::TopicAccountSizeGE(expectedAccountSize),
                                NLs::TopicUsedReserveSize(expectedUsedReserveSize)});
        };


        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 3
            PartitionPerTablet: 3
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 11
                    WriteSpeedInBytesPerSecond : 17
                }
                MeteringMode: METERING_MODE_REQUEST_UNITS
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(0, 0); // topic is empty

        ui64 balancerId = DescribePath(runtime, "/MyRoot/Topic1").GetPathDescription().GetPersQueueGroup().GetBalancerTabletID();

        auto stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetDataSize(), "DataSize from ReadBalancer");
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetUsedReserveSize(), "UsedReserveSize from ReadBalancer");

        auto msg = TString(24_MB, '_');

        ui32 msgSeqNo = 100;
        WriteToTopic(runtime, topicPath, ++msgSeqNo, msg);

        env.SimulateSleep(runtime, TDuration::Seconds(3)); // Wait TEvPeriodicTopicStats

        Assert(16975298, 0); // 16975298 - it is unstable value. it can change if internal message store change

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(16975298, stats->Record.GetDataSize(), "DataSize from ReadBalancer " << stats->Record.GetDataSize());
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetUsedReserveSize(), "UsedReserveSize from ReadBalancer " << stats->Record.GetUsedReserveSize());

        appData.PQConfig.SetBalancerWakeupIntervalSec(30);

        GracefulRestartTablet(runtime, balancerId, sender);

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(16975298, stats->Record.GetDataSize(), "DataSize from ReadBalancer after reload");
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetUsedReserveSize(), "UsedReserveSize from ReadBalancer after reload");
    }

    Y_UNIT_TEST(PeriodicTopicStatsReload) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto& appData = runtime.GetAppData();

        ui64 txId = 100;

        // disable batching
        appData.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
        appData.SchemeShardConfig.SetStatsMaxBatchSize(0);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        const auto AssertTopicSize = [&] (ui64 expectedAccountSize, ui64 expectedUsedReserveSize) {
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Topic1"),
                               {NLs::Finished,
                                NLs::TopicAccountSizeGE(expectedAccountSize),
                                NLs::TopicUsedReserveSize(expectedUsedReserveSize)});
        };

        TestCreatePQGroup(runtime, ++txId, "/MyRoot", R"(
            Name: "Topic1"
            TotalGroupCount: 1
            PartitionPerTablet: 1
            PQTabletConfig {
                PartitionConfig {
                    LifetimeSeconds: 1
                    WriteSpeedInBytesPerSecond : 7

                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        AssertTopicSize(7, 0);

        ui64 topic1Id = DescribePath(runtime, "/MyRoot/Topic1").GetPathDescription().GetSelf().GetPathId();

        ui64 generation = 1;
        ui64 round = 97;

        SendTEvPeriodicTopicStats(runtime, topic1Id, generation, round, 17, 7);
        AssertTopicSize(17, 7);

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        AssertTopicSize(17, 7); // loaded from db

        SendTEvPeriodicTopicStats(runtime, topic1Id, generation, round - 1, 19, 7);

        AssertTopicSize(17, 7); // not changed because round is less
    }

};

Y_UNIT_TEST_SUITE(TStoragePoolsStatsPersistence) {
    Y_UNIT_TEST(SameAggregatedStatsAfterRestart) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableBackgroundCompaction(false);
        TTestEnv env(runtime, opts);

        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
        NDataShard::gDbStatsDataSizeResolution = 1;
        NDataShard::gDbStatsRowCountResolution = 1;

        ui64 txId = 100;

        TVector<TString> poolsKinds;
        {
            auto databaseDescription = DescribePath(runtime, "/MyRoot").GetPathDescription().GetDomainDescription();
            UNIT_ASSERT_GE_C(databaseDescription.StoragePoolsSize(), 2u, databaseDescription.DebugString());
            for (const auto& pool : databaseDescription.GetStoragePools()) {
                poolsKinds.emplace_back(pool.GetKind());
            }
        }

        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
                    Name: "SomeTable"
                    Columns { Name: "key"   Type: "Uint32" FamilyName: "default"}
                    Columns { Name: "value" Type: "Utf8"   FamilyName: "alternative"}
                    KeyColumnNames: ["key"]
                    PartitionConfig {
                        ColumnFamilies {
                            Name: "default"
                            StorageConfig {
                                SysLog { PreferredPoolKind: "%s" }
                                Log { PreferredPoolKind: "%s" }
                                Data { PreferredPoolKind: "%s" }
                            }
                        }
                        ColumnFamilies {
                            Name: "alternative"
                            StorageConfig {
                                Data { PreferredPoolKind: "%s" }
                            }
                        }
                    }
                )", poolsKinds[0].c_str(), poolsKinds[0].c_str(), poolsKinds[0].c_str(), poolsKinds[1].c_str()
            )
        );
        env.TestWaitNotification(runtime, txId);

        auto shards = GetTableShards(runtime, "/MyRoot/SomeTable");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1);
        auto& datashard = shards[0];
        constexpr ui32 rowsCount = 100u;
        for (ui32 i = 0u; i < rowsCount; ++i) {
            UpdateRow(runtime, "SomeTable", i, "meaningless_value", datashard);
        }

        // compaction is necessary for storage pools' stats to appear in the periodic messages from datashards
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::CompactTable(runtime, datashard, ResolveTableId(runtime, "/MyRoot/SomeTable")).GetStatus(),
                                 NKikimrTxDataShard::TEvCompactTableResult::OK
        );
        // we wait for at least 1 part count, because it signals that the stats have been recalculated after compaction
        WaitTableStats(runtime, datashard, 1, rowsCount).GetTableStats();

        auto checkUsage = [&poolsKinds](ui64 totalUsage, const auto& poolUsage) {
            if (IsIn(poolsKinds, poolUsage.GetPoolKind())) {
                UNIT_ASSERT_GT_C(totalUsage, 0, poolUsage.DebugString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(totalUsage, 0, poolUsage.DebugString());
            }
        };
        const auto expectedTableStats = DescribePath(runtime, "/MyRoot/SomeTable").GetPathDescription().GetTableStats();
        UNIT_ASSERT_GT_C(expectedTableStats.GetStoragePools().PoolsUsageSize(), 0, expectedTableStats.DebugString());
        for (const auto& poolUsage : expectedTableStats.GetStoragePools().GetPoolsUsage()) {
            checkUsage(poolUsage.GetDataSize() + poolUsage.GetIndexSize(), poolUsage);
        }
        const auto expectedDatabaseStats = DescribePath(runtime, "/MyRoot").GetPathDescription().GetDomainDescription().GetDiskSpaceUsage();
        UNIT_ASSERT_GT_C(expectedDatabaseStats.StoragePoolsUsageSize(), 0, expectedDatabaseStats.DebugString());
        for (const auto& poolUsage : expectedDatabaseStats.GetStoragePoolsUsage()) {
            checkUsage(poolUsage.GetTotalSize(), poolUsage);
        }

        // will be used to turn off table stats processing in SchemeShard
        auto statsEventAction = TTestActorRuntime::EEventAction::PROCESS;
        SetStatsObserver(runtime, [&]() {
                return statsEventAction;
            }
        );

        // drop any further stat updates and restart SS
        // the only way for SS to know proper stat is to read it from localDB
        statsEventAction = TTestActorRuntime::EEventAction::DROP;

        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        auto tableStats = DescribePath(runtime, "/MyRoot/SomeTable").GetPathDescription().GetTableStats();
        auto databaseStats = DescribePath(runtime, "/MyRoot").GetPathDescription().GetDomainDescription().GetDiskSpaceUsage();

        using google::protobuf::util::MessageDifferencer;
        MessageDifferencer differencer;
        TString diff;
        differencer.ReportDifferencesToString(&diff);
        differencer.set_repeated_field_comparison(MessageDifferencer::RepeatedFieldComparison::AS_SET);
        UNIT_ASSERT_C(differencer.Compare(tableStats.GetStoragePools(), expectedTableStats.GetStoragePools()), diff);
        UNIT_ASSERT_C(differencer.Compare(databaseStats, expectedDatabaseStats), diff);
    }
}
