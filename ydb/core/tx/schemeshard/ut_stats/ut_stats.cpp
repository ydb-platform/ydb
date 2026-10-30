#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/public/lib/value/value.h>

#include <util/string/escape.h>

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

void SetPqrbTopicStatsSuppressor(TTestActorRuntime& runtime, ui32& allowInjectedTopicStats) {
    auto originalObserver = runtime.SetObserverFunc([](TAutoPtr<IEventHandle>&) {
        return TTestActorRuntime::EEventAction::PROCESS;
    });
    runtime.SetObserverFunc([originalObserver, &allowInjectedTopicStats](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPersQueue::EvPeriodicTopicStats) {
            if (allowInjectedTopicStats > 0) {
                --allowInjectedTopicStats;
                return TTestActorRuntime::EEventAction::PROCESS;
            }
            return TTestActorRuntime::EEventAction::DROP;
        }
        return originalObserver(ev);
    });
}

void InjectPeriodicTopicStats(
    TTestActorRuntime& runtime,
    ui32& allowInjectedTopicStats,
    ui64 topicId,
    ui64 generation,
    ui64 round,
    ui64 dataSize,
    ui64 usedReserveSize)
{
    ++allowInjectedTopicStats;
    SendTEvPeriodicTopicStats(runtime, topicId, generation, round, dataSize, usedReserveSize);
}

TTableId ResolveTableId(TTestActorRuntime& runtime, const TString& path) {
    auto response = Navigate(runtime, path);
    return response->ResultSet.at(0).TableId;
}

ui64 GetRowUpdates(TTestActorRuntime& runtime, const TString& path) {
    auto description = DescribePrivatePath(runtime, TTestTxConfig::SchemeShard, path, true, true);
    return description.GetPathDescription().GetTableStats().GetRowUpdates();
}

void WaitRowUpdatesAtLeast(TTestActorRuntime& runtime, TTestEnv& env, const TString& path, ui64 expected) {
    while (GetRowUpdates(runtime, path) < expected) {
        env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
    }
}

// A single-shard table whose writes go through the datashard write path, so they
// increment the RowUpdates counter (LocalMiniKQL writes do not).
void CreateSimpleTable(TTestActorRuntime& runtime, TTestEnv& env, ui64& txId) {
    TestCreateTable(runtime, TTestTxConfig::SchemeShard, ++txId, "/MyRoot", R"(
        Name: "Simple"
        Columns { Name: "key"   Type: "Uint32" }
        Columns { Name: "value" Type: "Utf8" }
        KeyColumnNames: ["key"]
    )");
    env.TestWaitNotification(runtime, txId);
}

void WriteRows(TTestActorRuntime& runtime, ui64& txId, int partitionIdx, ui32 fromKeyInclusive, ui32 toKey) {
    for (ui32 key = fromKeyInclusive; key < toKey; ++key) {
        WriteRow(runtime, ++txId, "/MyRoot/Simple", partitionIdx, key, "value");
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeshardStatsBatchingTest) {
    constexpr ui64 WRITTEN_TOPIC_DATA_SIZE = 16975298; // unstable value, can change if internal message store changes

    Y_UNIT_TEST(ShouldNotBatchWhenDisabled) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DataShardStatsReportIntervalSeconds(1);
        TTestEnv env(runtime, opts);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

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
        TTestEnvOptions opts;
        opts.DataShardStatsReportIntervalSeconds(1);
        TTestEnv env(runtime, opts);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

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
        TTestEnvOptions opts;
        opts.DataShardStatsReportIntervalSeconds(1);
        TTestEnv env(runtime, opts);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

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
        appData.PQConfig.SetBalancerWakeupIntervalSec(3600);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui32 allowInjectedTopicStats = 0;
        SetPqrbTopicStatsSuppressor(runtime, allowInjectedTopicStats);

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

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic1Id, generation, ++round, 101, 101);
        Assert(1369, 101); // only reserve size

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic1Id, generation, ++round, 383, 247);
        Assert(1369 + (383 - 247), 247); // 1505, 247 reserve + exceeding the limit

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic2Id, generation, ++round, 113, 113);
        Assert(1369 + (383 - 247), 247 + 113); // 1505, 360

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic1Id, generation, ++round, 31, 31);
        Assert(1369, 31 + 113); // only reserve, data size

        TestDropPQGroup(runtime, ++txId, "/MyRoot", "Topic2");
        env.TestWaitNotification(runtime, txId);
        Assert(808, 31);

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic3Id, generation, ++round, 151, 151);
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

        Assert(3 * 2678400 * 17, WRITTEN_TOPIC_DATA_SIZE);
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

        Assert(WRITTEN_TOPIC_DATA_SIZE, 0);

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(WRITTEN_TOPIC_DATA_SIZE, stats->Record.GetDataSize(), "DataSize from ReadBalancer " << stats->Record.GetDataSize());
        UNIT_ASSERT_EQUAL_C(0, stats->Record.GetUsedReserveSize(), "UsedReserveSize from ReadBalancer " << stats->Record.GetUsedReserveSize());

        appData.PQConfig.SetBalancerWakeupIntervalSec(30);

        GracefulRestartTablet(runtime, balancerId, sender);

        stats = NPQ::GetReadBalancerPeriodicTopicStats(runtime, balancerId);
        UNIT_ASSERT_EQUAL_C(WRITTEN_TOPIC_DATA_SIZE, stats->Record.GetDataSize(), "DataSize from ReadBalancer after reload");
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
        appData.PQConfig.SetBalancerWakeupIntervalSec(3600);

        // apply config via reboot
        TActorId sender = runtime.AllocateEdgeActor();

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui32 allowInjectedTopicStats = 0;
        SetPqrbTopicStatsSuppressor(runtime, allowInjectedTopicStats);

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

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic1Id, generation, round, 17, 7);
        AssertTopicSize(17, 7);

        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);

        AssertTopicSize(17, 7); // loaded from db

        InjectPeriodicTopicStats(runtime, allowInjectedTopicStats, topic1Id, generation, round - 1, 19, 7);

        AssertTopicSize(17, 7); // not changed because round is less
    }

    Y_UNIT_TEST(RowUpdatesSurviveShardRestart) {
        // A datashard keeps RowUpdates in memory, so a restart resets it to zero.
        // The schemeshard re-baselines the shard on the generation bump, so the table
        // aggregate must keep growing across the restart, never drop.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

        ui64 txId = 1000;
        CreateSimpleTable(runtime, env, txId);
        WriteRows(runtime, txId, 0, 0, INITIAL_ROWS_COUNT);

        WaitRowUpdatesAtLeast(runtime, env, "/MyRoot/Simple", INITIAL_ROWS_COUNT);

        // Restart the datashard: its in-memory RowUpdates counter resets to zero.
        auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, shards[0], sender);

        // Write more rows after the restart.
        const ui32 extraRows = 50;
        WriteRows(runtime, txId, 0, INITIAL_ROWS_COUNT, INITIAL_ROWS_COUNT + extraRows);

        // The aggregate keeps the pre-restart updates and adds the new ones; if it had
        // dropped on the generation bump it would stall at extraRows and never reach this.
        WaitRowUpdatesAtLeast(runtime, env, "/MyRoot/Simple", INITIAL_ROWS_COUNT + extraRows);
        UNIT_ASSERT_GE(GetRowUpdates(runtime, "/MyRoot/Simple"), INITIAL_ROWS_COUNT + extraRows);
    }

    Y_UNIT_TEST(RowUpdatesSurviveShardSplit) {
        // On split the schemeshard removes the parent shard and adds empty children.
        // RowUpdates is deliberately not subtracted with the parent's current-state
        // metrics, so the table aggregate survives the reshard.
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

        ui64 txId = 1000;
        // Single-shard table with keys [0, INITIAL_ROWS_COUNT).
        CreateSimpleTable(runtime, env, txId);
        WriteRows(runtime, txId, 0, 0, INITIAL_ROWS_COUNT);

        WaitRowUpdatesAtLeast(runtime, env, "/MyRoot/Simple", INITIAL_ROWS_COUNT);

        auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        // Split the single shard in two at key = INITIAL_ROWS_COUNT / 2.
        TestSplitTable(runtime, ++txId, "/MyRoot/Simple", Sprintf(R"(
                SourceTabletId: %lu
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint32: %lu } } } }
            )", shards[0], INITIAL_ROWS_COUNT / 2));
        env.TestWaitNotification(runtime, txId);

        auto newShards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple");
        UNIT_ASSERT_VALUES_EQUAL(newShards.size(), 2u);

        // Children start at zero, but the table aggregate still carries the parent's
        // updates — they were not lost in the reshard.
        UNIT_ASSERT_GE(GetRowUpdates(runtime, "/MyRoot/Simple"), INITIAL_ROWS_COUNT);
    }

};

Y_UNIT_TEST_SUITE(TStatsParseOffloadTest) {
    // Stats must round-trip through persist + restart the same regardless of the offload flag.
    Y_UNIT_TEST_FLAG(StatsPersistRegardlessOfOffload, EnablePeriodicTableStatsParseOffload) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.DataShardStatsReportIntervalSeconds(1);
        TTestEnv env(runtime, opts);

        auto& appData = runtime.GetAppData();
        appData.FeatureFlags.SetEnablePersistentPartitionStats(true);
        appData.FeatureFlags.SetEnablePeriodicTableStatsParseOffload(EnablePeriodicTableStatsParseOffload);

        auto eventAction = TTestActorRuntime::EEventAction::PROCESS;
        SetStatsObserver(runtime, [&]() {
                return eventAction;
            }
        );

        ui64 txId = 1000;
        CreateTableWithData(runtime, env, "/MyRoot", "Simple", 1, txId);

        WaitAndCheckStatPersisted(runtime, env, INITIAL_ROWS_COUNT, TDuration::Zero(), eventAction);
    }
}

Y_UNIT_TEST_SUITE(TStoragePoolsStatsPersistence) {
    Y_UNIT_TEST(DatabaseUsageAfterRestartDuringMoveWithShardIdxStats) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.NStoragePools(1);
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        TTestEnv env(runtime, opts);

        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableTablePartitionsFormatShardIdx(true);
        flags.SetEnableTablePartitionsFormatShardIdxByDefault(true);

        const auto sender = runtime.AllocateEdgeActor();
        auto observer = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>([&](auto& ev) {
            if (ev->Sender != sender) {
                ev.Reset();
            }
        });

        ui64 txId = 100;
        CreateSimpleTable(runtime, env, txId);
        const auto tableId = ResolveTableId(runtime, "/MyRoot/Simple");
        const auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/Simple");
        auto update = MakeHolder<TEvDataShard::TEvPeriodicTableStats>(
            shards.at(0), TTestTxConfig::SchemeShard, tableId.PathId.LocalPathId);
        update->Record.SetGeneration(1);
        update->Record.SetRound(1);
        update->Record.MutableTableStats()->SetDataSize(100);
        update->Record.MutableTableStats()->SetIndexSize(20);
        auto* channel = update->Record.MutableTableStats()->AddChannels();
        channel->SetChannel(1);
        channel->SetDataSize(100);
        channel->SetIndexSize(20);
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, update.Release());
        runtime.WaitFor("persisted source stats", [&] {
            return DescribePath(runtime, "/MyRoot/Simple").GetPathDescription().GetTableStats().GetDataSize() == 100;
        });

        const ui64 moveTxId = ++txId;
        TBlockEvents<NSchemeShard::TEvPrivate::TEvProgressOperation> blockedProgress(runtime, [=](const auto& ev) {
            return ev->Get()->TxId == moveTxId;
        });
        AsyncSend(runtime, TTestTxConfig::SchemeShard,
            MoveTableRequest(moveTxId, "/MyRoot/Simple", "/MyRoot/Moved"));
        TestModificationResult(runtime, moveTxId, NKikimrScheme::StatusAccepted);
        runtime.WaitFor("move progress", [&] { return !blockedProgress.empty(); });
        while (DescribePath(runtime, "/MyRoot/Moved").GetPathDescription().GetTableStats().GetDataSize() != 100) {
            blockedProgress.Unblock(1);
            runtime.WaitFor("move progress after next stage", [&] { return !blockedProgress.empty(); });
        }

        // Both table records still reference the shard, whose owner is now Moved.
        // Restart must restore only the owner's contribution to database usage.
        blockedProgress.clear();
        GracefulRestartTablet(runtime, TTestTxConfig::SchemeShard, sender);
        runtime.WaitFor("move progress after restart", [&] { return !blockedProgress.empty(); });
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {NLs::DatabaseSizeIs(120)});
        const auto usage = DescribePath(runtime, "/MyRoot").GetPathDescription()
            .GetDomainDescription().GetDiskSpaceUsage();
        UNIT_ASSERT_VALUES_EQUAL(usage.StoragePoolsUsageSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(usage.GetStoragePoolsUsage(0).GetTotalSize(), 120);

        blockedProgress.Stop().Unblock();
        env.TestWaitNotification(runtime, moveTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Simple"), {NLs::PathNotExist});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Moved"), {NLs::IsTable});
        TestDescribeResult(DescribePath(runtime, "/MyRoot"), {NLs::DatabaseSizeIs(120)});
    }

    Y_UNIT_TEST(DatabaseUsageAfterRestartWithMixedPartitionStatsFormats) {
        TTestBasicRuntime runtime;
        TTestEnvOptions opts;
        opts.NStoragePools(1);
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        TTestEnv env(runtime, opts);

        auto& flags = runtime.GetAppData().FeatureFlags;
        flags.SetEnableTablePartitionsFormatShardIdxByDefault(false);
        flags.SetEnableTablePartitionsFormatAutoConvert(false);

        // Only the explicitly injected update may change the restored stats.
        const auto sender = runtime.AllocateEdgeActor();
        auto observer = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>([&](auto& ev) {
            if (ev->Sender != sender) {
                ev.Reset();
            }
        });

        ui64 txId = 100;
        const ui64 schemeShard = TTestTxConfig::SchemeShard;
        const auto domain = DescribePath(runtime, "/MyRoot").GetPathDescription().GetDomainDescription();
        UNIT_ASSERT_GT(domain.StoragePoolsSize(), 0);
        const auto poolKind = domain.GetStoragePools(0).GetKind();
        const auto poolStats = [&](ui64 dataSize, ui64 indexSize) {
            NKikimrTableStats::TStoragePoolsStats stats;
            auto* pool = stats.AddPoolsUsage();
            pool->SetPoolKind(poolKind);
            pool->SetDataSize(dataSize);
            pool->SetIndexSize(indexSize);
            return EscapeC(stats.SerializeAsString());
        };

        auto persistStats = [&](const char* name, bool legacy, bool shardIdx, ui64 dataSize) {
            CreateTable(runtime, env, "/MyRoot", name, 1, txId);
            const auto tableId = ResolveTableId(runtime, TStringBuilder() << "/MyRoot/" << name);
            const auto localPathId = tableId.PathId.LocalPathId;

            const auto partition = LocalMiniKQL(runtime, schemeShard, Sprintf(R"(
                (
                    (let key '('('TabId (Uint64 '%lu)) '('Id (Uint64 '0))))
                    (return (AsList (SetResult 'Partition
                        (SelectRow 'TablePartitions key '('DatashardIdx)))))
                )
            )", localPathId));
            const ui64 localShardIdx = NClient::TValue::Create(partition)["Partition"]["DatashardIdx"];

            if (legacy) {
                LocalMiniKQL(runtime, schemeShard, Sprintf(R"(
                    (
                        (let key '('('TableOwnerId (Uint64 '%lu))
                            '('TableLocalId (Uint64 '%lu)) '('PartitionId (Uint64 '0))))
                        (let stats '('('SeqNoGeneration (Uint64 '1)) '('SeqNoRound (Uint64 '1))
                            '('DataSize (Uint64 '%lu)) '('IndexSize (Uint64 '10))
                            '('StoragePoolsStats (String '"%s"))))
                        (return (AsList (UpdateRow 'TablePartitionStats key stats)))
                    )
                )", schemeShard, localPathId, dataSize, poolStats(dataSize, 10).c_str()));
            }

            if (shardIdx) {
                // A newer row must override the legacy row before domain aggregation.
                LocalMiniKQL(runtime, schemeShard, Sprintf(R"(
                    (
                        (let key '('('OwnerPathId (Uint64 '%lu)) '('LocalPathId (Uint64 '%lu))
                            '('OwnerShardIdx (Uint64 '%lu)) '('LocalShardIdx (Uint64 '%lu))))
                        (let stats '('('SeqNoGeneration (Uint64 '1)) '('SeqNoRound (Uint64 '2))
                            '('DataSize (Uint64 '%lu)) '('IndexSize (Uint64 '20))
                            '('StoragePoolsStats (String '"%s"))))
                        (return (AsList (UpdateRow 'TablePartitionStatsByShardIdx key stats)))
                    )
                )", schemeShard, localPathId, schemeShard, localShardIdx, dataSize, poolStats(dataSize, 20).c_str()));
            }
        };

        persistStats("Mixed", true, true, 100);
        persistStats("Legacy", true, false, 50);
        persistStats("ShardIdx", false, true, 60);

        const auto checkUsage = [&](ui64 mixedDataSize) {
            for (const auto& [name, dataSize, indexSize] : {
                std::tuple{"Mixed", mixedDataSize, 20UL},
                std::tuple{"Legacy", 50UL, 10UL},
                std::tuple{"ShardIdx", 60UL, 20UL}})
            {
                const auto stats = DescribePath(runtime, TStringBuilder() << "/MyRoot/" << name)
                    .GetPathDescription().GetTableStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetDataSize(), dataSize);
                UNIT_ASSERT_VALUES_EQUAL(stats.GetIndexSize(), indexSize);
            }

            TestDescribeResult(DescribePath(runtime, "/MyRoot"),
                {NLs::DatabaseSizeIs(mixedDataSize + 20 + 60 + 80)});
            const auto usage = DescribePath(runtime, "/MyRoot").GetPathDescription()
                .GetDomainDescription().GetDiskSpaceUsage();
            UNIT_ASSERT_VALUES_EQUAL(usage.StoragePoolsUsageSize(), 1);
            const auto& pool = usage.GetStoragePoolsUsage(0);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetPoolKind(), poolKind);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetDataSize(), mixedDataSize + 50 + 60);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetIndexSize(), 20 + 10 + 20);
            UNIT_ASSERT_VALUES_EQUAL(pool.GetTotalSize(), mixedDataSize + 20 + 60 + 80);
        };

        for (ui32 restart = 0; restart < 2; ++restart) {
            GracefulRestartTablet(runtime, schemeShard, sender);
            checkUsage(100);
        }

        const auto tableId = ResolveTableId(runtime, "/MyRoot/Mixed");
        const auto shards = GetTableShards(runtime, schemeShard, "/MyRoot/Mixed");
        auto update = MakeHolder<TEvDataShard::TEvPeriodicTableStats>(
            shards.at(0), schemeShard, tableId.PathId.LocalPathId);
        update->Record.SetGeneration(1);
        update->Record.SetRound(3);
        update->Record.MutableTableStats()->SetDataSize(130);
        update->Record.MutableTableStats()->SetIndexSize(20);
        auto* channel = update->Record.MutableTableStats()->AddChannels();
        channel->SetChannel(1);
        channel->SetDataSize(130);
        channel->SetIndexSize(20);
        runtime.SendToPipe(schemeShard, sender, update.Release());

        while (DescribePath(runtime, "/MyRoot/Mixed").GetPathDescription().GetTableStats().GetDataSize() != 130) {
            env.SimulateSleep(runtime, TDuration::MilliSeconds(100));
        }
        checkUsage(130);

        GracefulRestartTablet(runtime, schemeShard, sender);
        checkUsage(130);
    }

    Y_UNIT_TEST(SameAggregatedStatsAfterRestart) {
        TTestBasicRuntime runtime;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        TTestEnvOptions opts;
        opts.DisableStatsBatching(true);
        opts.EnablePersistentPartitionStats(true);
        opts.EnableBackgroundCompaction(false);
        opts.DataShardStatsReportIntervalSeconds(0);
        TTestEnv env(runtime, opts);

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

        auto shards = GetTableShards(runtime, TTestTxConfig::SchemeShard, "/MyRoot/SomeTable");
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
        WaitTableStats(runtime, datashard, [](const NKikimrTableStats::TTableStats& stats) {
            return stats.GetPartCount() >= 1;
        });

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
