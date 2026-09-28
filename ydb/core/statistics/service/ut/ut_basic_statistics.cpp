#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NStat {

using namespace NYdb;
using namespace NYdb::NScheme;

namespace {

void FillTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    TStringBuilder replace;
    replace << Sprintf("REPLACE INTO `Root/%s/%s` (Key, Value) VALUES ",
        databaseName.c_str(), tableName.c_str());
    for (ui32 i = 0; i < rowCount; ++i) {
        if (i > 0) {
            replace << ", ";
        }
        replace << Sprintf("(%uu, %uu)", i, i);
    }
    replace << ";";
    ExecuteYqlScript(env, replace);
}

void CreateTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str()));
    FillTable(env, databaseName, tableName, rowCount);
}

void CreateTableWithGlobalIndex(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            INDEX ValueIndex GLOBAL ON ( Value ),
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str()));
    FillTable(env, databaseName, tableName, rowCount);
}

void WaitForStatsUpdateFromSchemeShard(
        TTestActorRuntime& runtime, ui64 ssTabletId, ui64 saTabletId) {
    bool statsUpdateSent = false;
    bool txnCommitted = false;
    auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev) {
        if (ev->Get()->Record.GetSchemeShardId() == ssTabletId) {
            statsUpdateSent = true;
        }
    });
    auto commitObserver = runtime.AddObserver<TEvTablet::TEvCommitResult>([&](auto& ev) {
        if (statsUpdateSent && ev->Get()->TabletID == saTabletId) {
            txnCommitted = true;
        }
    });
    // The SA may skip the DB write for an incomplete first stats report (no
    // commit), so wait for either a commit or just the event being received.
    runtime.WaitFor("stats update from SchemeShard", [&]{
        return txnCommitted || statsUpdateSent;
    });
}

void WaitForStatsPropagate(TTestActorRuntime& runtime, ui32 nodeIdx) {
    // First wait for the start of propagate round initiated by the aggregator,
    // then wait for it to arrive to the target node.
    bool propagateSentFromSA = false;
    bool propagateSentToNode = false;
    auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto& ev) {
        TActorId senderServiceId = runtime.GetLocalServiceId(
            MakeStatServiceID(ev->Sender.NodeId()),
            ev->Sender.NodeId() - runtime.GetFirstNodeId());
        if (ev->Sender != senderServiceId) {
            propagateSentFromSA = true;
        }
        if (propagateSentFromSA && ev->Recipient.NodeId() == runtime.GetNodeId(nodeIdx)) {
            propagateSentToNode = true;
        }
    });
    runtime.WaitFor("TEvPropagateStatistics", [&]{ return propagateSentToNode; });

}

} // namespace

Y_UNIT_TEST_SUITE(BasicStatistics) {
    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table", 5);

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoNodes) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database", 2);
        CreateTable(env, "Database", "Table", 5);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 2, pathId1, 5);
    }

    Y_UNIT_TEST(TwoTables) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table1", 5);
        CreateTable(env, "Database", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoDatabases) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database1", 1, false, "hdd1");
        CreateDatabase(env, "Database2", 1, false, "hdd2");
        CreateTable(env, "Database1", "Table1", 5);
        CreateTable(env, "Database2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database2/Table2");
        ValidateRowCount(runtime, 2, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(DedicatedTimeIntervals) {
        // Test that time intervals set in config for the serverless environment are honored.
        auto modifyConfig = [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsSendIntervalSecondsDedicated(3);
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsPropagateIntervalSecondsDedicated(3);
        };
        TTestEnv env(1, 2, false, modifyConfig);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database1", 1, false, "hdd1");
        CreateDatabase(env, "Database2", 1, false, "hdd2");
        CreateTable(env, "Database1", "Table1", 5);
        CreateTable(env, "Database2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database2/Table2");
        ValidateRowCount(runtime, 2, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);

        size_t sendCount = 0;
        auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto&){
            ++sendCount;
        });

        size_t propagateCount = 0;
        auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto&){
            ++propagateCount;
        });

        runtime.SimulateSleep(TDuration::Seconds(4));
        UNIT_ASSERT_GE(sendCount, 2); // at least one event from each tenant schemeshard
        UNIT_ASSERT_GE(propagateCount, 2); // at least one propagate event to each node
    }

    Y_UNIT_TEST(Serverless) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateTable(env, "Serverless", "Table", 5);

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoServerlessDbs) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoServerlessTwoSharedDbs) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared1", 1, true, "hdd1");
        CreateDatabase(env, "Shared2", 1, true, "hdd2");
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared1");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared2");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateRowCount(runtime, 2, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    void TestNotFullStatistics(TTestEnv& env, size_t shardCount, size_t expectedRowCount) {
        Y_ABORT_UNLESS(shardCount > 1, "Test expects more than 1 shard in the table");

        auto& runtime = *env.GetServer().GetRuntime();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
        ui64 ssTabletId = pathId.OwnerId;

        // Block stats updates from one of the shards and pass others through.
        // Datashards set TableLocalId/DatashardId at the top level of the record;
        // column shards put them in the repeated Tables field. We handle both.
        std::optional<ui64> blockedShardId;
        THashSet<ui64> updatedShardIds;
        auto blockPredicate = [&](const TEvDataShard::TEvPeriodicTableStats::TPtr& ev) {
            const auto& record = ev->Get()->Record;
            // Resolve the datashard id for this event. For datashards it's the
            // top-level field; for column shards it's in the matching Tables entry.
            ui64 datashardId = 0;
            if (record.GetTableLocalId() == pathId.LocalPathId) {
                datashardId = record.GetDatashardId();
            } else {
                for (const auto& table : record.GetTables()) {
                    if (table.GetTableLocalId() == pathId.LocalPathId) {
                        datashardId = table.GetDatashardId();
                        break;
                    }
                }
            }
            if (datashardId == 0) {
                return false;
            }
            if (!blockedShardId) {
                blockedShardId = datashardId;
                return true;
            } else if (blockedShardId == datashardId) {
                return true;
            } else {
                updatedShardIds.insert(datashardId);
                return false;
            }
        };
        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> blockShardStats(
            runtime, blockPredicate);

        runtime.WaitFor(
            "TEvPeriodicTableStats",
            [&]{ return updatedShardIds.size() >= shardCount - 1; });
        // Give SchemeShard time to process shard stats updates
        runtime.SimulateSleep(TDuration::Seconds(1));

        auto getDescribeRowCount = [&]() {
            auto sender = runtime.AllocateEdgeActor();
            auto describe = DescribeTable(runtime, sender, "/Root/Database/Table");
            return describe.GetPathDescription().GetTableStats().GetRowCount();
        };

        // Check that the row count in SchemeShard got partially updated.
        UNIT_ASSERT_GT(getDescribeRowCount(), 0);

        const ui32 nodeIdx = 1;

        // Check that the statistics service still reports 0 row count.
        WaitForStatsUpdateFromSchemeShard(runtime, ssTabletId, saTabletId);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId), 0);

        blockShardStats.Unblock();
        blockShardStats.Stop();

        // SendBaseStatsToSA may still emit a previously scheduled incomplete
        // blob; wait until StatService actually has the full row count.
        WaitForRowCount(runtime, nodeIdx, pathId, expectedRowCount);

        // Block updates from one of the shards again and reboot SchemeShard
        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> blockShardStatsAgain(
            runtime, blockPredicate);
        RebootTablet(runtime, ssTabletId, runtime.AllocateEdgeActor());
        updatedShardIds.clear();
        runtime.WaitFor(
            "TEvPeriodicTableStats2",
            [&]{ return updatedShardIds.size() >= shardCount - 1; });
        // Give SchemeShard time to process shard stats updates
        runtime.SimulateSleep(TDuration::Seconds(1));

        {
            // Check that the row count in SchemeShard got partially updated.
            ui64 rc = getDescribeRowCount();
            UNIT_ASSERT_GT(rc, 0);
            UNIT_ASSERT_LT(rc, expectedRowCount);
        }

        // Check that after an update from SchemeShard with incomplete stats for the table,
        // statistics service still reports correct row count.
        WaitForStatsUpdateFromSchemeShard(runtime, ssTabletId, saTabletId);
        WaitForStatsPropagate(runtime, nodeIdx);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId), expectedRowCount);
    }

    Y_UNIT_TEST(NotFullStatisticsDatashard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        PrepareUniformTable(env, "Database", "Table");

        TestNotFullStatistics(env, /*shardCount=*/ 4, /*expectedRowCount=*/ 4);
    }

    Y_UNIT_TEST(NotFullStatisticsColumnshard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        PrepareColumnTable(env, "Database", "Table", 4);

        TestNotFullStatistics(env, /*shardCount=*/ 4, /*expectedRowCount=*/ ColumnTableRowsNumber);
    }

    Y_UNIT_TEST(StatisticsOnShardsRestart) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        auto dbName =  "Database";
        auto table1 = "Table1";
        auto table2 = "Table2";
        auto table3 = "Table3";

        auto path1 = "/Root/Database/Table1";
        auto path2 = "/Root/Database/Table2";
        auto path3 = "/Root/Database/Table3";

        const ui32 nodeIdx = 1;
        ui64 saTabletId = 0;

        CreateDatabase(env, dbName);
        PrepareColumnTable(env, dbName, table1, 4);
        auto pathId1 = ResolvePathId(runtime, path1, nullptr, &saTabletId);

        ui64 ssTabletId = pathId1.OwnerId;
        auto sender = runtime.AllocateEdgeActor();

        auto getDescribeRowCount = [&](const TString& path) {
            auto describe = DescribeTable(runtime, sender, path);
            return describe.GetPathDescription().GetTableStats().GetRowCount();
        };

        WaitForSchemeShardStatsUpdate(runtime, ssTabletId, /*requireFull=*/true);
        WaitForRowCount(runtime, nodeIdx, pathId1, 1000, /*timeoutSec=*/30);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path1), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId1), 1000);

        auto ids = GetColumnTableShards(runtime, sender, path1);
        for (auto& id : ids) {
            RebootTablet(runtime, id, sender);
        }

        PrepareColumnTable(env, dbName, table2, 4);
        auto pathId2 = ResolvePathId(runtime, path2, nullptr, &saTabletId);

        WaitForSchemeShardStatsUpdate(runtime, ssTabletId, /*requireFull=*/true);
        WaitForRowCount(runtime, nodeIdx, pathId1, 1000, /*timeoutSec=*/30);
        WaitForRowCount(runtime, nodeIdx, pathId2, 1000, /*timeoutSec=*/30);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path1), 1000);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path2), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId1), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId2), 1000);

        RebootTablet(runtime, ssTabletId, runtime.AllocateEdgeActor());

        PrepareColumnTable(env, dbName, table3, 4);
        auto pathId3 = ResolvePathId(runtime, path3, nullptr, &saTabletId);

        WaitForSchemeShardStatsUpdate(runtime, ssTabletId, /*requireFull=*/true);
        WaitForRowCount(runtime, nodeIdx, pathId1, 1000, /*timeoutSec=*/30);
        WaitForRowCount(runtime, nodeIdx, pathId2, 1000, /*timeoutSec=*/30);
        WaitForRowCount(runtime, nodeIdx, pathId3, 1000, /*timeoutSec=*/30);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path1), 1000);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path2), 1000);
        UNIT_ASSERT_EQUAL(getDescribeRowCount(path3), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId1), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId2), 1000);
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId3), 1000);
    }

    Y_UNIT_TEST(SimpleGlobalIndex) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateTableWithGlobalIndex(env, "Database", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table/ValueIndex/indexImplTable");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(ServerlessGlobalIndex) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateTableWithGlobalIndex(env, "Serverless", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table/ValueIndex/indexImplTable");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(ServerlessTimeIntervals) {
        // Test that time intervals set in config for the serverless environment are honored.
        auto modifyConfig = [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsSendInitialDelaySeconds(5);
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsSendIntervalSecondsServerless(5);
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsPropagateIntervalSecondsServerless(5);
        };
        TTestEnv env(1, 1, false, modifyConfig);

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");

        THashMap<ui64, TVector<TInstant>> sendTimesBySs;
        auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev){
            auto ssId = ev->Get()->Record.GetSchemeShardId();
            if (ssId == pathId1.OwnerId || ssId == pathId2.OwnerId) {
                sendTimesBySs[ssId].push_back(runtime.GetCurrentTime());
            }
        });

        size_t propagateCount = 0;
        auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto&){
            ++propagateCount;
        });

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);

        auto enoughSamples = [&] {
            if (sendTimesBySs.size() != 2) {
                return false;
            }
            for (const auto& [_, times] : sendTimesBySs) {
                if (times.size() < 2) {
                    return false;
                }
            }
            return true;
        };
        for (int i = 0; i < 20 && !enoughSamples(); ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_VALUES_EQUAL(sendTimesBySs.size(), 2);
        for (const auto& [ssId, times] : sendTimesBySs) {
            UNIT_ASSERT_C(times.size() >= 2,
                "schemeshard " << ssId << " sent only " << times.size() << " time(s)");
            for (size_t i = 1; i < times.size(); ++i) {
                const auto gap = times[i] - times[i - 1];
                UNIT_ASSERT_C(gap >= TDuration::Seconds(3),
                    "schemeshard " << ssId << " gap " << gap << " is below serverless min jitter");
                UNIT_ASSERT_C(gap <= TDuration::Seconds(7),
                    "schemeshard " << ssId << " gap " << gap << " is above serverless max interval");
            }
        }
        UNIT_ASSERT_GE(propagateCount, 2);
    }

    Y_UNIT_TEST(PersistenceWithStorageFailuresAndReboots) {
        TTestEnv env(1, 2);
        auto& runtime = *env.GetServer().GetRuntime();

        const size_t rowCount1 = 5;

        CreateDatabase(env, "Database", 2);
        CreateTable(env, "Database", "Table", rowCount1);

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
        ui64 ssTabletId = pathId.OwnerId;

        const ui32 nodeIdx = 1;
        const ui32 otherNodeIdx = 2;

        // Block propagate events that go to node with otherNodeIdx. We will use this
        // node later as a clean slate.
        TBlockEvents<TEvStatistics::TEvPropagateStatistics> blockPropagate(runtime,
            [&](const TEvStatistics::TEvPropagateStatistics::TPtr& ev) {
                return ev->Recipient.NodeId() == runtime.GetNodeId(otherNodeIdx);
            });

        // Wait until correct statistics gets reported
        ValidateRowCount(runtime, nodeIdx, pathId, rowCount1);

        // Block persisting new updates from schemeshards on the aggregator.
        // This should result in old statistics being reported, even after new
        // updates arrive.
        TBlockEvents<TEvBlobStorage::TEvPut> blockPersistStats(runtime,
            [&](const TEvBlobStorage::TEvPut::TPtr& ev) {
                return ev->Get()->Id.TabletID() == saTabletId;
            });

        // Upsert some more data
        const size_t rowCount2 = 7;
        FillTable(env, "Database", "Table", rowCount2);

        {
            // Wait for an update from SchemeShard with new row count.

            bool statsUpdateSent = false;
            auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev){
                NKikimrStat::TSchemeShardStats statRecord;
                UNIT_ASSERT(statRecord.ParseFromString(ev->Get()->Record.GetStats()));
                for (const auto& entry : statRecord.GetEntries()) {
                    if (TPathId::FromProto(entry.GetPathId()) == pathId
                        && entry.GetAreStatsFull()
                        && entry.GetRowCount() == rowCount2) {
                        statsUpdateSent = true;
                    }
                }
            });
            runtime.WaitFor("TEvSchemeShardStats", [&]{ return statsUpdateSent; });

            // Give the aggregator time to (unsuccessfully) try to commit the update.
            runtime.SimulateSleep(TDuration::Seconds(1));

            bool propagateSent = false;
            auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto& ev){
                if (ev->Recipient.NodeId() == runtime.GetNodeId(nodeIdx)) {
                    propagateSent = true;
                }
            });
            runtime.WaitFor("TEvPropagateStatistics", [&]{ return propagateSent; });
        }
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, nodeIdx, pathId), rowCount1);

        TActorId sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, ssTabletId, sender);

        // Simulate storage failure, StatisticsAggregator will reboot.

        TBlockEvents<TEvStatistics::TEvSchemeShardStats> blockSSUpdates(runtime);
        UNIT_ASSERT_GT(blockPersistStats.size(), 0);
        blockPersistStats.Stop();
        for (auto& ev : blockPersistStats) {
            auto proxy = ev->Recipient;
            ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
            auto res = ev->Get()->MakeErrorResponse(
                NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
            ui32 nodeIdx = ev->Sender.NodeId() - runtime.GetFirstNodeId();
            runtime.Send(new IEventHandle(ev->Sender, proxy, res.release()), nodeIdx, true);
        }
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.emplace_back(TEvTablet::EvBoot);
        runtime.DispatchEvents(rebootOptions);

        // Check that after reboot the old value is still persisted by the Aggregator
        // and returned to the Service.
        blockPropagate.Stop();
        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, otherNodeIdx, pathId), rowCount1);

        // After everything is healed, stats should get updated.
        blockSSUpdates.Stop();
        WaitForRowCount(runtime, otherNodeIdx, pathId, rowCount2);
    }

    Y_UNIT_TEST(TableSummary) {
        TTestEnv env(1, 1);
        auto& runtime = *env.GetServer().GetRuntime();
        const size_t rowCount = 5;

        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table", rowCount);

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);
        Analyze(runtime, saTabletId, {pathId});

        auto responses = GetStatistics(runtime, pathId, EStatType::TABLE_SUMMARY, {std::nullopt});
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);

        const auto& resp = responses.at(0);
        UNIT_ASSERT(resp.Success);
        UNIT_ASSERT(resp.TableSummary.Data);
        UNIT_ASSERT_VALUES_EQUAL(resp.TableSummary.Data->GetRowCount(), rowCount);
    }
}

} // NSysView
} // NKikimr
