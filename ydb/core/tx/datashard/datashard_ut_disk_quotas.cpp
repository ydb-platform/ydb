#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/actors/block_events.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardDiskQuotas) {

    Y_UNIT_TEST(DiskQuotaExceeded) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            // .AddStoragePool("test", "/Root/db:test")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        // FIXME: subdomain creation hangs waiting for coordinators
        // Cerr << "... Creating subdomain" << Endl;
        // ui64 txId = AsyncCreateSubDomain(server, sender, "/Root", "db", R"(
        //         PlanResolution: 500
        //         Coordinators: 1
        //         Mediators: 1
        //         TimeCastBucketsPerMediator: 2
        //         StoragePools {
        //             Name: "/Root/db:test"
        //             Kind: "test"
        //         }
        //         DatabaseQuotas {
        //             data_size_hard_quota: 1
        //         }
        //     )");
        // WaitTxNotification(server, sender, txId);

        Cerr << "... Setting hard disk quota to 1 byte" << Endl;
        ui64 txId = AsyncAlterSubDomain(server, sender, "/", "Root", R"(
                StoragePools {
                    Name: "/Root:test"
                    Kind: "test"
                }
                DatabaseQuotas {
                    data_size_hard_quota: 1
                }
            )");
        WaitTxNotification(server, sender, txId);

        Cerr << "... Creating the table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        size_t observedZeroStats = 0;
        size_t observedNonZeroStats = 0;
        auto periodicStatsObserver = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>(
            [&](auto& ev) {
                auto* msg = ev->Get();
                if (msg->Record.GetTableStats().GetDataSize() > 0) {
                    ++observedNonZeroStats;
                } else {
                    ++observedZeroStats;
                }
            });

        Cerr << "... Inserting the 1st row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (1, 1);
                )"),
            "<empty>");

        runtime.WaitFor("non-zero stats", [&]{ return observedNonZeroStats > 0; });
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Note: this depends on schemeshard correctly bumping root path version
        Cerr << "... Inserting the 2nd row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table` (key, value) VALUES (2, 2);
                )"),
            "ERROR: UNAVAILABLE");
    }

    Y_UNIT_TEST(ShardRestartOnCreateTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        Cerr << "... Setting hard disk quota to 1 byte" << Endl;
        ui64 txId = AsyncAlterSubDomain(server, sender, "/", "Root", R"(
                StoragePools {
                    Name: "/Root:test"
                    Kind: "test"
                }
                DatabaseQuotas {
                    data_size_hard_quota: 1
                }
            )");
        WaitTxNotification(server, sender, txId);

        Cerr << "... Creating the 1st table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table1` (key int, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        size_t observedZeroStats = 0;
        size_t observedNonZeroStats = 0;
        auto periodicStatsObserver = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>(
            [&](auto& ev) {
                auto* msg = ev->Get();
                if (msg->Record.GetTableStats().GetDataSize() > 0) {
                    ++observedNonZeroStats;
                } else {
                    ++observedZeroStats;
                }
            });

        Cerr << "... Inserting the 1st row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table1` (key, value) VALUES (1, 1);
                )"),
            "<empty>");

        runtime.WaitFor("non-zero stats", [&]{ return observedNonZeroStats > 0; });
        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... Inserting the 2nd row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table1` (key, value) VALUES (2, 2);
                )"),
            "ERROR: UNAVAILABLE");

        Cerr << "... Creating the 2nd table" << Endl;
        ui64 tabletId = 0;
        auto createTabletObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>(
            [&](auto& ev) {
                tabletId = ev->Get()->Record.GetTabletID();
            });
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime, [&](auto& ev) {
            return ev->Get()->Record.GetTabletID() == tabletId;
        });
        auto createTableFuture = KqpSchemeExecSend(runtime, R"(
            CREATE TABLE `/Root/table2` (key int, value int, PRIMARY KEY (key));
            )");
        runtime.WaitFor("blocked plan", [&]{ return blockedPlan.size() >= 1; });
        blockedPlan.Stop();

        Cerr << "... Restarting shard " << tabletId << Endl;
        RebootTablet(runtime, tabletId, sender);
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExecWait(runtime, std::move(createTableFuture)),
            "SUCCESS");

        Cerr << "... Inserting the 3rd row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table2` (key, value) VALUES (3, 3);
                )"),
            "ERROR: UNAVAILABLE");

        Cerr << "... Dropping the 1st table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                DROP TABLE `/Root/table1`;
            )"),
            "SUCCESS");

        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... Inserting the 4th row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table2` (key, value) VALUES (4, 4);
                )"),
            "<empty>");
    }

    Y_UNIT_TEST(ShardRestartOnSplitDst) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);

        Cerr << "... Setting hard disk quota to 1 byte" << Endl;
        ui64 txId = AsyncAlterSubDomain(server, sender, "/", "Root", R"(
                StoragePools {
                    Name: "/Root:test"
                    Kind: "test"
                }
                DatabaseQuotas {
                    data_size_hard_quota: 1
                }
            )");
        WaitTxNotification(server, sender, txId);

        Cerr << "... Creating tables" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                CREATE TABLE `/Root/table1` (key uint32, value int, PRIMARY KEY (key));
                CREATE TABLE `/Root/table2` (key uint32, value int, PRIMARY KEY (key));
            )"),
            "SUCCESS");

        size_t observedZeroStats = 0;
        size_t observedNonZeroStats = 0;
        auto periodicStatsObserver = runtime.AddObserver<TEvDataShard::TEvPeriodicTableStats>(
            [&](auto& ev) {
                auto* msg = ev->Get();
                if (msg->Record.GetTableStats().GetDataSize() > 0) {
                    ++observedNonZeroStats;
                } else {
                    ++observedZeroStats;
                }
            });

        Cerr << "... Inserting the 1st row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table1` (key, value) VALUES (1, 1);
                )"),
            "<empty>");

        runtime.WaitFor("non-zero stats", [&]{ return observedNonZeroStats > 0; });
        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... Inserting the 2nd row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table2` (key, value) VALUES (2, 2);
                )"),
            "ERROR: UNAVAILABLE");

        TVector<ui64> tabletIds;
        auto createTabletObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>(
            [&](auto& ev) {
                tabletIds.push_back(ev->Get()->Record.GetTabletID());
            });

        TBlockEvents<TEvDataShard::TEvSplitTransferSnapshot> blockedSnapshots(runtime);

        Cerr << "... Splitting the 2nd table" << Endl;
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        auto shards2before = GetTableShards(server, sender, "/Root/table2");
        ui64 txId2 = AsyncSplitTable(server, sender, "/Root/table2", shards2before.at(0), 10);
        runtime.WaitFor("blocked snapshots", [&]{ return blockedSnapshots.size() >= 2; });
        blockedSnapshots.Stop();

        runtime.SimulateSleep(TDuration::Seconds(1));

        // Reboot dst tablets
        createTabletObserver.Remove();
        for (ui64 tabletId : tabletIds) {
            Cerr << "... Restarting shard " << tabletId << Endl;
            RebootTablet(runtime, tabletId, sender);
        }

        // Wait for split to finish
        Cerr << "... Waiting for split to finish" << Endl;
        WaitTxNotification(server, sender, txId2);

        // Shards should still be blocked from writes
        Cerr << "... Inserting the 3rd row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table2` (key, value) VALUES (3, 3);
                )"),
            "ERROR: UNAVAILABLE");

        Cerr << "... Dropping the 1st table" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSchemeExec(runtime, R"(
                DROP TABLE `/Root/table1`;
            )"),
            "SUCCESS");

        runtime.SimulateSleep(TDuration::Seconds(1));

        Cerr << "... Inserting the 4th row" << Endl;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                UPSERT INTO `/Root/table2` (key, value) VALUES (4, 4);
                )"),
            "<empty>");
    }

} // Y_UNIT_TEST_SUITE(DataShardDiskQuotas)

} // namespace NKikimr
