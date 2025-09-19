#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

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

    void TestNotFullStatistics(TTestEnv& env, size_t expectedRowCount) {
        auto& runtime = *env.GetServer().GetRuntime();

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");

        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> block(runtime);
        runtime.WaitFor("TEvPeriodicTableStats", [&]{ return block.size() >= 3; });
        block.Unblock(3);

        bool firstStatsToSA = false;
        auto statsObserver1 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto&){
            firstStatsToSA = true;
        });
        runtime.WaitFor("TEvSchemeShardStats 1", [&]{ return firstStatsToSA; });

        UNIT_ASSERT(GetRowCount(runtime, 1, pathId) == 0);

        block.Unblock();
        block.Stop();

        bool secondStatsToSA = false;
        auto statsObserver2 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto&){
            secondStatsToSA = true;
        });
        runtime.WaitFor("TEvSchemeShardStats 2", [&]{ return secondStatsToSA; });

        size_t propagateCount = 0;
        auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto&){
            ++propagateCount;
        });
        runtime.WaitFor("TEvPropagateStatistics", [&]{ return propagateCount >= runtime.GetNodeCount(); });

        UNIT_ASSERT_VALUES_EQUAL(GetRowCount(runtime, 1, pathId), expectedRowCount);
    }

    Y_UNIT_TEST(NotFullStatisticsDatashard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        TestNotFullStatistics(env, 4);
    }

    Y_UNIT_TEST(NotFullStatisticsColumnshard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateColumnStoreTable(env, "Database", "Table", 4);

        TestNotFullStatistics(env, 1000);
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
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsSendIntervalSecondsServerless(30);
            settings.AppConfig->MutableStatisticsConfig()->SetBaseStatsPropagateIntervalSecondsServerless(30);
        };
        TTestEnv env(1, 1, false, modifyConfig);

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        // Wait until reported row counts are correct.
        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);

        // Subsequent events renewing base statistics should not be sent out for a long time.

        size_t sendCount = 0;
        auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev){
            // Count only events from serverless schemeshards.
            NKikimrStat::TSchemeShardStats stats;
            UNIT_ASSERT(stats.ParseFromString(ev->Get()->Record.GetStats()));
            if (stats.GetEntries().size() == 1) {
                auto ownerId = stats.GetEntries()[0].GetPathId().GetOwnerId();
                if (ownerId == pathId1.OwnerId || ownerId == pathId2.OwnerId) {
                    ++sendCount;
                }
            }
        });

        size_t propagateCount = 0;
        auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto&){
            ++propagateCount;
        });

        runtime.SimulateSleep(TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(sendCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(propagateCount, 0);

        runtime.SimulateSleep(TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(sendCount, 2); // events from 2 serverless schemeshards
        UNIT_ASSERT_VALUES_EQUAL(propagateCount, 2); // SA -> node1 and node1 -> node2
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
}

} // NSysView
} // NKikimr
