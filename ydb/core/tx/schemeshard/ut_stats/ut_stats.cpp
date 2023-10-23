#include <ydb/core/cms/console/console.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/datashard/datashard.h>

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

        // capture original observer func by setting dummy one
        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvPeriodicTableStats: {
                return eventAction;
            }
            default:
                return originalObserver(runtime, ev);
            }
        });

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

        // capture original observer func by setting dummy one
        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvPeriodicTableStats: {
                ++statsCount;
                return eventAction;
            }
            default:
                return originalObserver(runtime, ev);
            }
        });

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

        // capture original observer func by setting dummy one
        auto originalObserver = runtime.SetObserverFunc([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        // now set our observer backed up by original
        runtime.SetObserverFunc([&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvPeriodicTableStats: {
                return eventAction;
            }
            default:
                return originalObserver(runtime, ev);
            }
        });

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

        TestDeallocatePQ(runtime, ++txId, "/MyRoot", "Name: \"Topic3\"");
        env.TestWaitNotification(runtime, txId);
        Assert(247, 31);
    }

    Y_UNIT_TEST(TopicPeriodicStatMeteringModeReserved) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_TRACE);
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
                    LifetimeSeconds: 11
                    WriteSpeedInBytesPerSecond : 17
                }
                MeteringMode: METERING_MODE_RESERVED_CAPACITY
            }
        )");
        env.TestWaitNotification(runtime, txId);
        Assert(3 * 11 * 17, 0); // 561, 0 

        ui32 msgSeqNo = 100;
        WriteToTopic(runtime, topicPath, msgSeqNo, "Message 100");

        env.SimulateSleep(runtime, TDuration::Seconds(3)); // Wait TEvPeriodicTopicStats

        Assert(3 * 11 * 17, 69); // 69 - it is unstable value. it can change if internal message store change
    }

    Y_UNIT_TEST(TopicPeriodicStatMeteringModeRequest) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

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

        ui32 msgSeqNo = 100;
        WriteToTopic(runtime, topicPath, msgSeqNo, "Message 100");

        env.SimulateSleep(runtime, TDuration::Seconds(3)); // Wait TEvPeriodicTopicStats

        Assert(69, 0); //  69 - it is unstable value. it can change if internal message store change

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(69, stats->Record.GetDataSize(), "DataSize from ReadBalancer");
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetUsedReserveSize(), "UsedReserveSize from ReadBalancer");

        appData.PQConfig.SetBalancerWakeupIntervalSec(30);

        GracefulRestartTablet(runtime, balancerId, sender);

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(69, stats->Record.GetDataSize(), "DataSize from ReadBalancer after reload");
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
                                NLs::TopicAccountSize(expectedAccountSize),
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
