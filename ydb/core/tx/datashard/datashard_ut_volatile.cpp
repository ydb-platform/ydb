#include "datashard_ut_common.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_ut_common_pq.h"
#include "datashard_active_transaction.h"

#include <ydb/core/kqp/executer_actor/kqp_executer.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardVolatile) {

    Y_UNIT_TEST(DistributedWrite) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        Cerr << "!!! distributed write begin" << Endl;
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )");
        Cerr << "!!! distributed write end" << Endl;

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } }");

        const auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        RebootTablet(runtime, shards1.at(0), sender);

        // We should see same results after restart
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } }");
    }

    Y_UNIT_TEST(DistributedWriteBrokenLock) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        // Start transaction that reads from table-1 and table-2
        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
            )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");

        // Break lock using a blind write to table-1
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3);
                UPSERT INTO `/Root/table-2` (key, value) VALUES (30, 30);
            )"),
            "ERROR: ABORTED");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Verify transaction was not committed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");
    }

    Y_UNIT_TEST(DistributedWriteShardRestartBeforePlan) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        TVector<THolder<IEventHandle>> capturedPlans;
        auto capturePlans = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvPlanStep::EventType: {
                    Cerr << "... captured TEvPlanStep" << Endl;
                    capturedPlans.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(capturePlans);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        Cerr << "!!! distributed write begin" << Endl;
        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedPlans.size() >= 2; }, "both captured plans");

        runtime.SetObserverFunc(prevObserverFunc);

        // Restart the first table shard
        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        RebootTablet(runtime, shards1.at(0), sender);

        // Unblock captured plan messages
        for (auto& ev : capturedPlans) {
            runtime.Send(ev.Release(), 0, true);
        }
        capturedPlans.clear();

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "ERROR: UNDETERMINED");
        Cerr << "!!! distributed write end" << Endl;

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Verify transaction was not committed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");
    }

    Y_UNIT_TEST(DistributedWriteShardRestartAfterExpectation) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);

        TVector<THolder<IEventHandle>> capturedPlans;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvPlanStep::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    if (msg->Record.GetTabletID() == shard1) {
                        Cerr << "... captured TEvPlanStep for " << msg->Record.GetTabletID() << Endl;
                        capturedPlans.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    if (msg->Record.GetTabletDest() == shard1) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        Cerr << "!!! distributed write begin" << Endl;
        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedPlans.size() >= 1 && capturedReadSets.size() >= 2; }, "captured plan and readsets");

        runtime.SetObserverFunc(prevObserverFunc);

        // Restart the first table shard
        RebootTablet(runtime, shard1, sender);

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "ERROR: UNDETERMINED");
        Cerr << "!!! distributed write end" << Endl;

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Verify transaction was not committed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");
    }

    Y_UNIT_TEST(DistributedWriteEarlierSnapshotNotBlocked) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        TString sessionIdSnapshot, txIdSnapshot;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionIdSnapshot, txIdSnapshot, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
            )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest()
                        << " with flags " << msg->Record.GetFlags() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        Cerr << "!!! distributed write begin" << Endl;
        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");

        runtime.SetObserverFunc(prevObserverFunc);
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Make sure snapshot transaction cannot see uncommitted changes and doesn't block on them
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionIdSnapshot, txIdSnapshot, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
            )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");
    }

    Y_UNIT_TEST(DistributedWriteLaterSnapshotBlockedThenCommit) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest()
                        << " with flags " << msg->Record.GetFlags() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        Cerr << "!!! distributed write begin" << Endl;
        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");

        runtime.SetObserverFunc(prevObserverFunc);
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        TString sessionIdSnapshot = CreateSessionRPC(runtime, "/Root");
        auto snapshotReadFuture = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            SELECT key, value FROM `/Root/table-1`
            UNION ALL
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key
        )", sessionIdSnapshot, "", false /* commitTx */), "/Root");

        // Let some virtual time pass
        SimulateSleep(runtime, TDuration::Seconds(1));

        // Read should be blocked, so we don't expect a reply
        UNIT_ASSERT(!snapshotReadFuture.HasValue());

        // Unblock readsets and sleep some more
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }
        SimulateSleep(runtime, TDuration::Seconds(1));

        // We expect successful commit and read including that data
        UNIT_ASSERT(snapshotReadFuture.HasValue());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(future.ExtractValue()),
            "<empty>");
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(snapshotReadFuture.ExtractValue()),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } }");
    }

    Y_UNIT_TEST(DistributedWriteLaterSnapshotBlockedThenAbort) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NLog::PRI_DEBUG);

        size_t observedPlans = 0;
        TVector<THolder<IEventHandle>> capturedPlans;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvPlanStep::EventType: {
                    ++observedPlans;
                    const auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    if (msg->Record.GetTabletID() == shard1) {
                        Cerr << "... captured TEvPlanStep for " << msg->Record.GetTabletID() << Endl;
                        capturedPlans.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        Cerr << "!!! distributed write begin" << Endl;
        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return observedPlans >= 2; }, "observed both plans");
        UNIT_ASSERT_VALUES_EQUAL(capturedPlans.size(), 1u);

        runtime.SetObserverFunc(prevObserverFunc);
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Start reading from table-2
        TString sessionIdSnapshot = CreateSessionRPC(runtime, "/Root");
        auto snapshotReadFuture = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key
        )", sessionIdSnapshot, "", false /* commitTx */), "/Root");

        // Let some virtual time pass
        SimulateSleep(runtime, TDuration::Seconds(1));

        // Read should be blocked, so we don't expect a reply
        UNIT_ASSERT(!snapshotReadFuture.HasValue());
        UNIT_ASSERT(!future.HasValue());

        // Reboot table-1 tablet and sleep a little, this will abort the write
        RebootTablet(runtime, shard1, sender);
        SimulateSleep(runtime, TDuration::Seconds(1));

        // We expect aborted commit and read without that data
        UNIT_ASSERT(snapshotReadFuture.HasValue());
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(future.ExtractValue()),
            "ERROR: UNDETERMINED");
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(snapshotReadFuture.ExtractValue()),
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }");
    }

    Y_UNIT_TEST(DistributedWriteAsymmetricExecute) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);

        size_t observedPlans = 0;
        TVector<THolder<IEventHandle>> capturedPlans;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvPlanStep::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    ++observedPlans;
                    if (msg->Record.GetTabletID() == shard1) {
                        Cerr << "... captured TEvPlanStep for " << msg->Record.GetTabletID() << Endl;
                        capturedPlans.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return observedPlans >= 2; }, "observed plans");
        UNIT_ASSERT_VALUES_EQUAL(capturedPlans.size(), 1u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Wait until it completes at shard2
        SimulateSleep(runtime, TDuration::Seconds(1));

        // Unblock plan at shard1
        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedPlans) {
            runtime.Send(ev.Release(), 0, true);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "<empty>");
        Cerr << "!!! distributed write end" << Endl;
    }

    Y_UNIT_TEST(DistributedWriteThenDropTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        size_t observedPropose = 0;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    ++observedPropose;
                    Cerr << "... observed TEvProposeTransaction" << Endl;
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        observedPropose = 0;
        ui64 txId = AsyncDropTable(server, sender, "/Root", "table-1");
        WaitFor(runtime, [&]{ return observedPropose > 0; }, "observed propose");

        SimulateSleep(runtime, TDuration::Seconds(1));

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitTxNotification(server, sender, txId);
    }

    Y_UNIT_TEST(DistributedWriteThenImmediateUpsert) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 2, 42);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Note: this upsert happens over the upsert into the value column
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value2) VALUES (2, 51);");

        // This compaction verifies there's no commit race with the waiting
        // distributed transaction. If commits happen in incorrect order we
        // would observe unexpected results.
        CompactTable(runtime, shard1, tableId1, false);

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "<empty>");

        // Verify the result
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                UNION ALL
                SELECT key, value, value2 FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } items { uint32_value: 51 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } items { null_flag_value: NULL_VALUE } }");
    }

    Y_UNIT_TEST(DistributedWriteThenCopyTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        size_t observedPropose = 0;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    ++observedPropose;
                    Cerr << "... observed TEvProposeTransaction" << Endl;
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        observedPropose = 0;
        ui64 txId = AsyncCreateCopyTable(server, sender, "/Root", "table-1-copy", "/Root/table-1");
        WaitFor(runtime, [&]{ return observedPropose > 0; }, "observed propose");

        SimulateSleep(runtime, TDuration::Seconds(1));

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        // Wait for copy table to finish
        WaitTxNotification(server, sender, txId);

        // Verify table copy has above changes committed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1-copy`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    Y_UNIT_TEST(DistributedWriteThenSplit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        size_t observedSplit = 0;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvSplit::EventType: {
                    ++observedSplit;
                    Cerr << "... observed TEvSplit" << Endl;
                    break;
                }
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                    capturedReadSets.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        SetSplitMergePartCountLimit(server->GetRuntime(), -1);
        auto shards1before = GetTableShards(server, sender, "/Root/table-1");
        ui64 txId = AsyncSplitTable(server, sender, "/Root/table-1", shards1before.at(0), 2);
        WaitFor(runtime, [&]{ return observedSplit > 0; }, "observed split");

        SimulateSleep(runtime, TDuration::Seconds(1));

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        // Wait for split to finish
        WaitTxNotification(server, sender, txId);

        // Verify table has changes committed
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    Y_UNIT_TEST(DistributedWriteThenReadIterator) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 2, 42);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        // Note: observer works strangely with edge actor results, so we use a normal actor here
        TVector<THolder<IEventHandle>> readResults;
        auto readSender = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvReadResult::EventType: {
                    Cerr << "... observed TEvReadResult:" << Endl;
                    Cerr << ev->Get<TEvDataShard::TEvReadResult>()->Record.DebugString() << Endl;
                    readResults.emplace_back(ev.Release());
                    break;
                }
                default: {
                    Cerr << "... ignore event " << ev->GetTypeRewrite() << Endl;
                }
            }
        }));

        {
            auto msg = std::make_unique<TEvDataShard::TEvRead>();
            msg->Record.SetReadId(1);
            msg->Record.MutableTableId()->SetOwnerId(tableId1.PathId.OwnerId);
            msg->Record.MutableTableId()->SetTableId(tableId1.PathId.LocalPathId);
            msg->Record.MutableTableId()->SetSchemaVersion(tableId1.SchemaVersion);
            msg->Record.MutableSnapshot()->SetStep(maxReadSetStep);
            msg->Record.MutableSnapshot()->SetTxId(Max<ui64>());
            msg->Record.AddColumns(1);
            msg->Record.AddColumns(2);
            msg->Record.SetResultFormat(NKikimrTxDataShard::ARROW);

            TVector<TCell> fromKeyCells = { TCell::Make(ui32(0)) };
            TVector<TCell> toKeyCells = { TCell::Make(ui32(10)) };
            auto fromBuf = TSerializedCellVec::Serialize(fromKeyCells);
            auto toBuf = TSerializedCellVec::Serialize(toKeyCells);
            msg->Ranges.emplace_back(fromBuf, toBuf, true, true);

            ForwardToTablet(runtime, shard1, readSender, msg.release());
        }

        // Since key=2 is not committed we must not observe results yet
        SimulateSleep(runtime, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(readResults.size(), 0u);

        captureReadSets = false;
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitFor(runtime, [&]{ return readResults.size() > 0; }, "read result");
        UNIT_ASSERT_VALUES_EQUAL(readResults.size(), 1u);

        {
            auto* msg = readResults[0]->Get<TEvDataShard::TEvReadResult>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetArrowBatch()->ToString(),
                "key:   [\n"
                "    1,\n"
                "    2\n"
                "  ]\n"
                "value:   [\n"
                "    1,\n"
                "    2\n"
                "  ]\n");
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(DistributedWriteThenReadIteratorStream) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 2, 42);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        // Note: observer works strangely with edge actor results, so we use a normal actor here
        TVector<THolder<IEventHandle>> readResults;
        auto readSender = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvReadResult::EventType: {
                    Cerr << "... observed TEvReadResult:" << Endl;
                    Cerr << ev->Get<TEvDataShard::TEvReadResult>()->Record.DebugString() << Endl;
                    readResults.emplace_back(ev.Release());
                    break;
                }
                default: {
                    Cerr << "... ignore event " << ev->GetTypeRewrite() << Endl;
                }
            }
        }));

        {
            auto msg = std::make_unique<TEvDataShard::TEvRead>();
            msg->Record.SetReadId(1);
            msg->Record.MutableTableId()->SetOwnerId(tableId1.PathId.OwnerId);
            msg->Record.MutableTableId()->SetTableId(tableId1.PathId.LocalPathId);
            msg->Record.MutableTableId()->SetSchemaVersion(tableId1.SchemaVersion);
            msg->Record.MutableSnapshot()->SetStep(maxReadSetStep);
            msg->Record.MutableSnapshot()->SetTxId(Max<ui64>());
            msg->Record.AddColumns(1);
            msg->Record.AddColumns(2);
            msg->Record.SetResultFormat(NKikimrTxDataShard::ARROW);
            msg->Record.SetMaxRowsInResult(1);

            TVector<TCell> fromKeyCells = { TCell::Make(ui32(0)) };
            TVector<TCell> toKeyCells = { TCell::Make(ui32(10)) };
            auto fromBuf = TSerializedCellVec::Serialize(fromKeyCells);
            auto toBuf = TSerializedCellVec::Serialize(toKeyCells);
            msg->Ranges.emplace_back(fromBuf, toBuf, true, true);

            ForwardToTablet(runtime, shard1, readSender, msg.release());
        }

        // We expect to receive key=1 as soon as possible since it's committed
        // However further data should not be available so soon
        SimulateSleep(runtime, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(readResults.size(), 1u);

        // Verify we actually receive key=1
        {
            auto* msg = readResults[0]->Get<TEvDataShard::TEvReadResult>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetArrowBatch()->ToString(),
                "key:   [\n"
                "    1\n"
                "  ]\n"
                "value:   [\n"
                "    1\n"
                "  ]\n");
            readResults.clear();
        }

        // Unblock readsets and let key=2 to commit
        captureReadSets = false;
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        WaitFor(runtime, [&]{ return readResults.size() > 0; }, "read result");
        UNIT_ASSERT_GE(readResults.size(), 1u);

        {
            auto* msg = readResults[0]->Get<TEvDataShard::TEvReadResult>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(msg->GetArrowBatch()->ToString(),
                "key:   [\n"
                "    2\n"
                "  ]\n"
                "value:   [\n"
                "    2\n"
                "  ]\n");

            msg = readResults.back()->Get<TEvDataShard::TEvReadResult>();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetFinished(), true);
        }
    }

    Y_UNIT_TEST(DistributedWriteThenScanQuery) {
        TPortManager pm;
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(false);
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        std::vector<TString> observedResults;
        TMaybe<Ydb::StatusIds::StatusCode> observedStatus;
        auto scanSender = runtime.Register(new TLambdaActor([&](TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
            switch (ev->GetTypeRewrite()) {
                case NKqp::TEvKqpExecuter::TEvStreamData::EventType: {
                    auto* msg = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>();
                    Cerr << "... observed stream data" << Endl;
                    if (msg->Record.GetResultSet().rows().size()) {
                        observedResults.emplace_back(FormatResult(msg->Record.GetResultSet()));
                    }
                    auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                    resp->Record.SetSeqNo(msg->Record.GetSeqNo());
                    resp->Record.SetFreeSpace(1);
                    ctx.Send(ev->Sender, resp.Release());
                    break;
                }
                case NKqp::TEvKqp::TEvQueryResponse::EventType: {
                    auto* msg = ev->Get<NKqp::TEvKqp::TEvQueryResponse>();
                    Cerr << "... observed query result" << Endl;
                    observedStatus = msg->Record.GetRef().GetYdbStatus();
                    break;
                }
                default: {
                    Cerr << "... ignored event " << ev->GetTypeRewrite();
                    if (ev->HasEvent()) {
                        Cerr << " " << ev->GetTypeName();
                    }
                    Cerr << Endl;
                }
            }
        }));

        SendRequest(runtime, scanSender, MakeStreamRequest(scanSender, R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key;
        )"));

        SimulateSleep(runtime, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(observedResults.size(), 0u);

        captureReadSets = false;
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        SimulateSleep(runtime, TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(observedResults.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(
            observedResults[0],
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
        UNIT_ASSERT_VALUES_EQUAL(observedStatus, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(DistributedWriteWithAsyncIndex) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                        })
                        .Indexes({
                            {"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                        });
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 3);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 30);
        )");
        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Make sure changes are actually delivered
        SimulateSleep(runtime, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, R"(
                SELECT key, value
                FROM `/Root/table-1` VIEW by_value
                ORDER BY key
            )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 3 } }");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleStaleRoExec(runtime, R"(
                SELECT key, value
                FROM `/Root/table-2` VIEW by_value
                ORDER BY key
            )"),
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 30 } }");
    }

    Y_UNIT_TEST(DistributedWriteThenBulkUpsert) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 2, 42);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Write to key 2 using bulk upsert
        NThreading::TFuture<Ydb::Table::BulkUpsertResponse> bulkUpsertFuture;
        {
            Ydb::Table::BulkUpsertRequest request;
            request.set_table("/Root/table-1");
            auto* r = request.mutable_rows();

            auto* reqRowType = r->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
            auto* reqKeyType = reqRowType->add_members();
            reqKeyType->set_name("key");
            reqKeyType->mutable_type()->set_type_id(Ydb::Type::UINT32);
            auto* reqValueType = reqRowType->add_members();
            reqValueType->set_name("value");
            reqValueType->mutable_type()->set_type_id(Ydb::Type::UINT32);

            auto* reqRows = r->mutable_value();
            auto* row1 = reqRows->add_items();
            row1->add_items()->set_uint32_value(2);
            row1->add_items()->set_uint32_value(22);

            using TEvBulkUpsertRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
                Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
            bulkUpsertFuture = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));

            auto response = AwaitResponse(runtime, std::move(bulkUpsertFuture));
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        // This compaction verifies there's no commit race with the waiting
        // distributed transaction. If commits happen in incorrect order we
        // would observe unexpected results.
        const auto shard1 = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        CompactTable(runtime, shard1, tableId1, false);

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "<empty>");

        // Verify the result
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                UNION ALL
                SELECT key, value, value2 FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } items { uint32_value: 42 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } items { null_flag_value: NULL_VALUE } }");
    }

    namespace {
        using TCdcStream = TShardedTableOptions::TCdcStream;

        TCdcStream NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormat format, const TString& name = "Stream") {
            return TCdcStream{
                .Name = name,
                .Mode = NKikimrSchemeOp::ECdcStreamModeNewAndOldImages,
                .Format = format,
            };
        }

        TCdcStream WithInitialScan(TCdcStream streamDesc) {
            streamDesc.InitialState = NKikimrSchemeOp::ECdcStreamStateScan;
            return streamDesc;
        }

        void WaitForContent(TServer::TPtr server, const TActorId& sender, const TString& path, const TVector<TString>& expected) {
            while (true) {
                const auto records = GetPqRecords(*server->GetRuntime(), sender, path, 0);
                if (records.size() >= expected.size()) {
                    for (ui32 i = 0; i < expected.size(); ++i) {
                        UNIT_ASSERT_VALUES_EQUAL(records.at(i).second, expected.at(i));
                    }

                    UNIT_ASSERT_VALUES_EQUAL(records.size(), expected.size());
                    break;
                }

                SimulateSleep(server, TDuration::Seconds(1));
            }
        }
    } // namespace

    Y_UNIT_TEST(DistributedWriteThenBulkUpsertWithCdc) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000)
            .SetEnableChangefeedInitialScan(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false},
                            {"value2", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        WaitTxNotification(server, sender, AsyncAlterAddStream(server, "/Root", "table-1",
            WithInitialScan(NewAndOldImages(NKikimrSchemeOp::ECdcStreamFormatJson))));

        WaitForContent(server, sender, "/Root/table-1/Stream", {
            R"({"update":{},"newImage":{"value2":null,"value":1},"key":[1]})",
        });

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(true);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value, value2) VALUES (2, 2, 42);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", true /* commitTx */), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        runtime.GetAppData(0).FeatureFlags.SetEnableDataShardVolatileTransactions(false);

        // Write to key 2 using bulk upsert
        NThreading::TFuture<Ydb::Table::BulkUpsertResponse> bulkUpsertFuture;
        {
            Ydb::Table::BulkUpsertRequest request;
            request.set_table("/Root/table-1");
            auto* r = request.mutable_rows();

            auto* reqRowType = r->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
            auto* reqKeyType = reqRowType->add_members();
            reqKeyType->set_name("key");
            reqKeyType->mutable_type()->set_type_id(Ydb::Type::UINT32);
            auto* reqValueType = reqRowType->add_members();
            reqValueType->set_name("value");
            reqValueType->mutable_type()->set_type_id(Ydb::Type::UINT32);

            auto* reqRows = r->mutable_value();
            auto* row1 = reqRows->add_items();
            row1->add_items()->set_uint32_value(2);
            row1->add_items()->set_uint32_value(22);

            using TEvBulkUpsertRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
                Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
            bulkUpsertFuture = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));

            // Note: we expect bulk upsert to block until key 2 outcome is decided
            SimulateSleep(runtime, TDuration::Seconds(1));
            UNIT_ASSERT(!bulkUpsertFuture.HasValue());
        }

        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : capturedReadSets) {
            runtime.Send(ev.Release(), 0, true);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "<empty>");

        SimulateSleep(runtime, TDuration::Seconds(1));

        // Verify the result
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value, value2 FROM `/Root/table-1`
                UNION ALL
                SELECT key, value, value2 FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } items { uint32_value: 42 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } items { null_flag_value: NULL_VALUE } }");

        WaitForContent(server, sender, "/Root/table-1/Stream", {
            R"({"update":{},"newImage":{"value2":null,"value":1},"key":[1]})",
            R"({"update":{},"newImage":{"value2":42,"value":2},"key":[2]})",
            R"({"update":{},"newImage":{"value2":42,"value":22},"key":[2],"oldImage":{"value2":42,"value":2}})",
        });

        UNIT_ASSERT(bulkUpsertFuture.HasValue());
    }

    Y_UNIT_TEST(DistributedWriteThenLateWriteReadCommit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetDomainPlanResolution(1000)
            .SetEnableKqpImmediateEffects(true)
            .SetEnableDataShardVolatileTransactions(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
                        .Shards(1)
                        .Columns({
                            {"key", "Uint32", true, false},
                            {"value", "Uint32", false, false}});
        CreateShardedTable(server, sender, "/Root", "table-1", opts);
        CreateShardedTable(server, sender, "/Root", "table-2", opts);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (10, 10);");

        ui64 maxReadSetStep = 0;
        bool captureReadSets = true;
        TVector<THolder<IEventHandle>> capturedReadSets;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    maxReadSetStep = Max(maxReadSetStep, msg->Record.GetStep());
                    if (captureReadSets) {
                        Cerr << "... captured TEvReadSet for " << msg->Record.GetTabletDest() << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

        TString sessionId = CreateSessionRPC(runtime, "/Root");

        auto future = SendRequest(runtime, MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (20, 20);
        )", sessionId, "", /* commitTx */ true), "/Root");

        WaitFor(runtime, [&]{ return capturedReadSets.size() >= 4; }, "captured readsets");
        UNIT_ASSERT_VALUES_EQUAL(capturedReadSets.size(), 4u);

        // Make an uncommitted write and read it to make sure it's flushed to datashard
        TString sessionId2 = CreateSessionRPC(runtime, "/Root");
        TString txId2;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId2, txId2, R"(
                UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 22);
            )"),
            "<empty>");
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleContinue(runtime, sessionId2, txId2, R"(
                SELECT key, value FROM `/Root/table-1` WHERE key = 2;
            )"),
            "{ items { uint32_value: 2 } items { uint32_value: 22 } }");

        // Unblock readsets and let it commit
        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : std::exchange(capturedReadSets, {})) {
            runtime.Send(ev.Release(), 0, true);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(AwaitResponse(runtime, std::move(future))),
            "<empty>");

        // Commit the transaction, it should succeed without crashing
        UNIT_ASSERT_VALUES_EQUAL(
            CommitTransactionRPC(runtime, sessionId2, txId2),
            "");

        // Verify the result
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/table-1`
                UNION ALL
                SELECT key, value FROM `/Root/table-2`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 22 } }, "
            "{ items { uint32_value: 10 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 20 } }");
    }

} // Y_UNIT_TEST_SUITE(DataShardVolatile)

} // namespace NKikimr
