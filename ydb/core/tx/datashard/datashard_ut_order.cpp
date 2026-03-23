#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"
#include "datashard_active_transaction.h"

#include <ydb/core/protos/query_stats.pb.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/library/actors/util/memory_tracker.h>
#include <util/system/valgrind.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

using TActiveTxPtr = std::shared_ptr<TActiveTransaction>;

///
class TDatashardTester {
public:
    static TActiveTxPtr MakeEmptyTx(ui64 step, ui64 txId) {
        TBasicOpInfo op(txId, EOperationKind::ReadTable, 0, Max<ui64>(), TInstant(), 0);
        op.SetStep(step);
        return std::make_shared<TActiveTransaction>(op);
    }
};


///
Y_UNIT_TEST_SUITE(TxOrderInternals) {

Y_UNIT_TEST(OperationOrder) {
    using TTester = TDatashardTester;

    TActiveTxPtr tx0_100 = TTester::MakeEmptyTx(0, 100);
    TActiveTxPtr tx0_101 = TTester::MakeEmptyTx(0, 101);
    TActiveTxPtr tx1_40 = TTester::MakeEmptyTx(1, 40);
    TActiveTxPtr tx1_102 = TTester::MakeEmptyTx(1, 102);
    TActiveTxPtr tx1_103 = TTester::MakeEmptyTx(1, 103);
    TActiveTxPtr tx2_42 = TTester::MakeEmptyTx(2, 42);

    UNIT_ASSERT_EQUAL(tx0_100->GetStepOrder().CheckOrder(tx0_101->GetStepOrder()), ETxOrder::Any);
    UNIT_ASSERT_EQUAL(tx0_101->GetStepOrder().CheckOrder(tx0_100->GetStepOrder()), ETxOrder::Any);

    UNIT_ASSERT_EQUAL(tx0_100->GetStepOrder().CheckOrder(tx1_102->GetStepOrder()), ETxOrder::Unknown);
    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx0_100->GetStepOrder()), ETxOrder::Unknown);

    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx1_103->GetStepOrder()), ETxOrder::Before);
    UNIT_ASSERT_EQUAL(tx1_103->GetStepOrder().CheckOrder(tx1_102->GetStepOrder()), ETxOrder::After);

    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx1_40->GetStepOrder()), ETxOrder::After);
    UNIT_ASSERT_EQUAL(tx1_102->GetStepOrder().CheckOrder(tx2_42->GetStepOrder()), ETxOrder::Before);
}

}


Y_UNIT_TEST_SUITE(DataShardOutOfOrder) {

Y_UNIT_TEST(TestOutOfOrderLockLost) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    auto sender3Session = CreateSessionRPC(runtime);
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    }

    // Now send a simple request that would upsert a new value into table-1
    // It would have broken locks if executed before the above commit
    // However the above commit must succeed (readsets are already being exchanged)
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), sender3Session, "", true));

    // Schedule a simple timer to simulate some time passing
    {
        auto sender4 = runtime.AllocateEdgeActor();
        runtime.Schedule(new IEventHandle(sender4, sender4, new TEvents::TEvWakeup()), TDuration::Seconds(1));
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender4);
    }

    // Whatever happens we should resend blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the immediate reply first, it must always succeed
    {
        auto response = AwaitResponse(runtime, f3);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Read the commit reply next
    bool committed;
    {
        auto response = AwaitResponse(runtime, f2);
        if (response.operation().status() == Ydb::StatusIds::ABORTED) {
            // Let's suppose somehow locks still managed to become invalidated
            committed = false;
        } else {
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            committed = true;
        }
    }

    // Select keys 3 and 4 from both tables, either both or none should be inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        TString expected;
        if (committed) {
            expected = "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }";
        } else {
            expected = "";
        }
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }
}

Y_UNIT_TEST(TestOutOfOrderReadOnlyAllowed) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    }

    // Now send a simple read request from table-1
    // Since it's readonly it cannot affect inflight transaction and should be allowed
    // Note: volatile transactions execute immediately and reads are blocked until resolved
    if (!usesVolatileTxs) {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` ORDER BY key"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
    }

    // Whatever happens we should resend blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the commit reply next, it must succeed
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Select keys 3 and 4 from both tables, both should have been be inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestOutOfOrderNonConflictingWrites) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    bool blockReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (blockReadSets && event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Now send non-conflicting upsert to both tables
    {
        blockReadSets = false;  // needed for volatile transactions
        auto result = KqpSimpleExec(runtime, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 3))"));
        UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
        blockReadSets = true; // restore to blocking
    }

    // Check that immediate non-conflicting upsert is working too
    {
        auto result = KqpSimpleExec(runtime, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 4)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "<empty>");
    }

    // Resend previousy blocked readsets
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Read the commit reply next, it must succeed
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Select keys 3 and 4 from both tables, both should have been inserted
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(
            $rows = (
                SELECT key, value FROM `/Root/table-1` WHERE key = 3
                UNION ALL
                SELECT key, value FROM `/Root/table-2` WHERE key = 4
            );
            SELECT key, value FROM $rows ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }, { items { uint32_value: 4 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestOutOfOrderRestartLocksReorderedWithoutBarrier) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // This test requires barrier to be disabled
    runtime.GetAppData().FeatureFlags.SetDisableDataShardBarrier(true);

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    bool blockReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        if (blockReadSets && event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            readSets.push_back(std::move(event));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Execute some out-of-order upserts before rebooting
    blockReadSets = false; // needed for volatile transactions
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 3))"));
    blockReadSets = true; // restore back to blocking

    // Select key 3, we expect a timeout, because logically writes
    // to 3 and 5 already happened, but physically write to 3 is still waiting.
    {
        TString tmpSessionId = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"), tmpSessionId, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::TIMEOUT);
    }

    // Reboot table-1 tablet
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    readSets.clear();
    RebootTablet(runtime, table1shards[0], sender);

    // Wait until we captured both readsets again
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_C(readSets.size() >= expectedReadSets, "expected " << readSets.size() << " >= " << expectedReadSets);

    // Select key 3, we still expect a timeout
    {
        TString tmpSessionId = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"), tmpSessionId, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::TIMEOUT);
    }

    // Select key 5, it shouldn't pose any problems
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 5;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 5 } items { uint32_value: 3 } }");
    }

    // Release readsets allowing tx to progress
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* viaActorSystem */ true);
    }

    // Select key 3, we expect a success
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 3;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 3 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestOutOfOrderNoBarrierRestartImmediateLongTail) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetAppConfig(app)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // This test requires barrier to be disabled
    runtime.GetAppData().FeatureFlags.SetDisableDataShardBarrier(true);

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);
    TString sessionId3 = CreateSessionRPC(runtime);
    TString sessionId4 = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    THashMap<TActorId, ui64> actorToTablet;
    TVector<THolder<IEventHandle>> readSets;
    TVector<THolder<IEventHandle>> progressEvents;
    bool blockReadSets = true;
    bool blockProgressEvents = false;
    size_t bypassProgressEvents = 0;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        auto recipient = event->GetRecipientRewrite();
        switch (event->GetTypeRewrite()) {
            case TEvTablet::EvBoot: {
                auto* msg = event->Get<TEvTablet::TEvBoot>();
                auto tabletId = msg->TabletID;
                Cerr << "... found " << recipient << " to be tablet " << tabletId << Endl;
                actorToTablet[recipient] = tabletId;
                break;
            }
            case TEvTxProcessing::EvReadSet: {
                if (blockReadSets) {
                    readSets.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 0 /* EvProgressTransaction */: {
                ui64 tabletId = actorToTablet.Value(recipient, 0);
                if (blockProgressEvents && tabletId == table1shards[0]) {
                    if (bypassProgressEvents == 0) {
                        Cerr << "... captured TEvProgressTransaction" << Endl;
                        progressEvents.push_back(std::move(event));
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... bypass for TEvProgressTransaction" << Endl;
                    --bypassProgressEvents;
                }
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto fCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    blockReadSets = false; // needed when upsert is volatile

    // Send some more requests that form a staircase, they would all be blocked as well
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3), (5, 3);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 3), (6, 3))"), sessionId3, "", true));
    SimulateSleep(server, TDuration::Seconds(1));
    auto f4 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 4), (7, 4);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (6, 4), (8, 4))"), sessionId4, "", true));
    SimulateSleep(server, TDuration::Seconds(1));

    // One more request that would be executed out of order
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (11, 5);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (12, 5))"));

    blockReadSets = true; // restore after a volatile upsert

    // Select key 7, we expect a timeout, because logically a write to it already happened
    // Note: volatile upserts are not blocked however, so it will succeed
    {
        auto sender4 = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"), sender4, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), usesVolatileTxs ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::TIMEOUT);
    }

    // Reboot table-1 tablet
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    readSets.clear();
    blockProgressEvents = true;
    bypassProgressEvents = 1;
    Cerr << "... rebooting tablet" << Endl;
    RebootTablet(runtime, table1shards[0], sender);
    Cerr << "... tablet rebooted" << Endl;

    // Wait until we captured both readsets again
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_C(readSets.size() >= expectedReadSets, "expected " << readSets.size() << " >= " << expectedReadSets);

    // Wait until we have a pending progress event
    if (usesVolatileTxs) {
        // Transaction does not restart when volatile (it's already executed)
        SimulateSleep(server, TDuration::Seconds(1));
    } else {
        if (progressEvents.size() < 1) {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                [&](IEventHandle &) -> bool {
                    return progressEvents.size() >= 1;
                });
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(progressEvents.size(), 1u);
    }

    // Select key 7 again, we still expect a timeout, because logically a write to it already happened
    // Note: volatile upserts are not blocked however, so it will succeed
    {
        auto sender5 = CreateSessionRPC(runtime);
        auto req = MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"), sender5, "", true);
        req.mutable_operation_params()->mutable_operation_timeout()->set_seconds(1);
        auto response = AwaitResponse(runtime, SendRequest(runtime, std::move(req)));
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), usesVolatileTxs ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::TIMEOUT);
    }

    // Stop blocking readsets and unblock progress
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, true);
    }
    for (auto& ev : progressEvents) {
        runtime.Send(ev.Release(), 0, true);
    }

    // Select key 7 again, this time is should succeed
    {
        auto result = KqpSimpleExec(runtime, Q_("SELECT key, value FROM `/Root/table-1` WHERE key = 7;"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 7 } items { uint32_value: 4 } }");
    }
}

Y_UNIT_TEST(TestPlannedTimeoutSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWrite1 = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);
    auto shards2 = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1u);

    // Capture and block some messages
    TVector<THolder<IEventHandle>> txProposes;
    auto captureMessages = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProxy::EvProposeTransaction: {
                Cerr << "---- observed EvProposeTransaction ----" << Endl;
                txProposes.push_back(std::move(event));
                return TTestActorRuntime::EEventAction::DROP;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureMessages);

    // Send a distributed write while capturing coordinator propose
    auto fWrite1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (101, 101);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (202, 202);
    )"), senderWrite1, "", true));
    if (txProposes.size() < 1) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return txProposes.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(txProposes.size(), 1u);
    runtime.SetObserverFunc(prevObserverFunc);

    size_t observedSplits = 0;
    auto observeSplits = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit: {
                Cerr << "---- observed EvSplit ----" << Endl;
                ++observedSplits;
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observeSplits);

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1 and table-2
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId1 = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1[0], 100);
    ui64 txId2 = AsyncSplitTable(server, senderSplit, "/Root/table-2", shards2[0], 100);

    // Wait until we observe both splits on both shards
    if (observedSplits < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return observedSplits >= 2;
            });
        runtime.DispatchEvents(options);
    }

    // Sleep a little so everything settles
    SimulateSleep(server, TDuration::Seconds(1));

    // We expect splits to finish successfully
    WaitTxNotification(server, senderSplit, txId1);
    WaitTxNotification(server, senderSplit, txId2);

    // We expect split to fully succeed on proposed transaction timeout
    auto shards1new = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1new.size(), 2u);
    auto shards2new = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2new.size(), 2u);

    // Unblock previously blocked coordinator propose
    for (auto& ev : txProposes) {
        runtime.Send(ev.Release(), 0, true);
    }

    // Wait for query to return an error
    {
        auto response = AwaitResponse(runtime, fWrite1);
        UNIT_ASSERT_C(
            response.operation().status() == Ydb::StatusIds::ABORTED ||
            response.operation().status() == Ydb::StatusIds::UNAVAILABLE,
            "Status: " << response.operation().status());
    }
}

Y_UNIT_TEST(TestPlannedHalfOverloadedSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    NKikimrConfig::TAppConfig app;
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWrite1 = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);
    auto shards2 = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1u);
    TVector<ui64> tablets;
    tablets.push_back(shards1[0]);
    tablets.push_back(shards2[0]);

    // Capture and block some messages
    TVector<THolder<IEventHandle>> txProposes;
    TVector<THolder<IEventHandle>> txProposeResults;
    auto captureMessages = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvProposeTransaction:
            case NKikimr::NEvents::TDataEvents::EvWrite: {
                Cerr << "---- observed EvProposeTransactionResult ----" << Endl;
                if (txProposes.size() == 0) {
                    // Capture the first propose
                    txProposes.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::EvProposeTransactionResult:
            case NKikimr::NEvents::TDataEvents::EvWriteResult: {
                Cerr << "---- observed EvProposeTransactionResult ----" << Endl;
                if (txProposes.size() > 0) {
                    // Capture all propose results
                    txProposeResults.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureMessages);

    // Send a distributed write while capturing coordinator propose
    auto fWrite1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (101, 101);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (202, 202);
    )"), senderWrite1, "", true));
    if (txProposes.size() < 1 || txProposeResults.size() < 1) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return txProposes.size() >= 1 && txProposeResults.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(txProposes.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(txProposeResults.size(), 1u);

    size_t observedSplits = 0;
    auto observeSplits = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit: {
                Cerr << "---- observed EvSplit ----" << Endl;
                ++observedSplits;
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    runtime.SetObserverFunc(observeSplits);

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1 and table-2
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId1 = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1[0], 100);
    ui64 txId2 = AsyncSplitTable(server, senderSplit, "/Root/table-2", shards2[0], 100);

    // Wait until we observe both splits on both shards
    if (observedSplits < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return observedSplits >= 2;
            });
        runtime.DispatchEvents(options);
    }

    // Sleep a little so everything settles
    SimulateSleep(server, TDuration::Seconds(1));

    // Unblock previously blocked proposes and results
    for (auto& ev : txProposeResults) {
        runtime.Send(ev.Release(), 0, true);
    }
    for (auto& ev : txProposes) {
        runtime.Send(ev.Release(), 0, true);
    }

    // We expect splits to finish successfully
    WaitTxNotification(server, senderSplit, txId1);
    WaitTxNotification(server, senderSplit, txId2);

    // We expect split to fully succeed on proposed transaction timeout
    auto shards1new = GetTableShards(server, sender, "/Root/table-1");
    UNIT_ASSERT_VALUES_EQUAL(shards1new.size(), 2u);
    auto shards2new = GetTableShards(server, sender, "/Root/table-2");
    UNIT_ASSERT_VALUES_EQUAL(shards2new.size(), 2u);

    // Wait for query to return an error
    {
        auto response = AwaitResponse(runtime, fWrite1);
        UNIT_ASSERT_C(
            response.operation().status() == Ydb::StatusIds::ABORTED ||
            response.operation().status() == Ydb::StatusIds::OVERLOADED ||
            response.operation().status() == Ydb::StatusIds::UNAVAILABLE,
            "Status: " << response.operation().status());
    }
}

namespace {

    void AsyncReadTable(
            Tests::TServer::TPtr server,
            TActorId sender,
            const TString& path)
    {
        auto &runtime = *server->GetRuntime();

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetStreamResponse(true);
        auto &tx = *request->Record.MutableTransaction()->MutableReadTableTransaction();
        tx.SetPath(path);
        tx.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
        tx.AddColumns("key");
        tx.AddColumns("value");

        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    }

}

/**
 * Regression test for KIKIMR-7751, designed to crash under asan
 */
Y_UNIT_TEST(TestReadTableWriteConflict) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    // NOTE: table-1 has 2 shards so ReadTable is not immediate
    CreateShardedTable(server, sender, "/Root", "table-1", 2);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    TString senderWriteImm = CreateSessionRPC(runtime);
    TString senderWriteDist = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(
            "SELECT * FROM `/Root/table-1` "
            "UNION ALL "
            "SELECT * FROM `/Root/table-2` "
            "ORDER BY key"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
             "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
             "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    TVector<THolder<IEventHandle>> txProposes;
    size_t seenPlanSteps = 0;
    bool captureReadSets = true;
    auto captureRS = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            case TEvTxProcessing::EvReadSet:
                Cerr << "---- observed EvReadSet ----" << Endl;
                if (captureReadSets) {
                    readSets.push_back(std::move(event));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto qCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2); "
        "UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2); "), sessionId, txId, true));

    // Wait until we captured all readsets
    const size_t expectedReadSets = usesVolatileTxs ? 8 : 4;
    if (readSets.size() < expectedReadSets) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return readSets.size() >= expectedReadSets;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);
    captureReadSets = false;

    // Start reading table-1, wait for its plan step
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);
    seenPlanSteps = 0;

    // Start an immediate write to table-1, it won't be able to start
    // because it arrived after the read table and they block each other
    auto fWriteImm = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), senderWriteImm, "", true));

    // Start a planned write to both tables, wait for its plan step too
    auto fWriteDist = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 4); "
        "UPSERT INTO `/Root/table-2` (key, value) VALUES (8, 4)"), senderWriteDist, "", true));
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);
    seenPlanSteps = 0;

    // Make sure everything is settled down
    SimulateSleep(server, TDuration::Seconds(1));

    // Unblock readsets, letting everything go
    for (auto& ev : readSets) {
        runtime.Send(ev.Release(), 0, /* via actor system */ true);
    }
    readSets.clear();

    // Wait for commit to succeed
    {
        auto response = AwaitResponse(runtime, qCommit);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Wait for immediate write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteImm);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Wait for distributed write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteDist);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

/**
 * Regression test for KIKIMR-7903
 */
Y_UNIT_TEST(TestReadTableImmediateWriteBlock) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    // NOTE: table-1 has 2 shards so ReadTable is not immediate
    CreateShardedTable(server, sender, "/Root", "table-1", 2);

    TString senderWriteImm = CreateSessionRPC(runtime);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));

    // Capture and block all readset messages
    size_t seenPlanSteps = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            case TEvTxProcessing::EvStreamClearanceResponse:
                Cerr << "---- dropped EvStreamClearanceResponse ----" << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    // Start reading table-1, wait for its plan step
    // Since we drop EvStreamClearanceResponse it will block forever
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");
    if (seenPlanSteps < 2) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle &) -> bool {
                return seenPlanSteps >= 2;
            });
        runtime.DispatchEvents(options);
    }
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 2u);

    // Make sure everything is settled down
    SimulateSleep(server, TDuration::Seconds(1));

    // Start an immediate write to table-1, it should be able to complete
    auto fWriteImm = SendRequest(runtime, MakeSimpleRequestRPC(Q_(
        "UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 3)"), senderWriteImm, "", true));

    // Wait for immediate write to succeed
    {
        auto response = AwaitResponse(runtime, fWriteImm);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(TestReadTableSingleShardImmediate) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));

    // Capture and block all readset messages
    size_t seenPlanSteps = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvTxProcessing::EvPlanStep:
                Cerr << "---- observed EvPlanStep ----" << Endl;
                ++seenPlanSteps;
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    // Start reading table-1
    seenPlanSteps = 0;
    auto senderReadTable = runtime.AllocateEdgeActor();
    AsyncReadTable(server, senderReadTable, "/Root/table-1");

    // Wait for the first quota request
    runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvStreamQuotaRequest>(senderReadTable);

    // Since ReadTable was for a single-shard table we shouldn't see any plan steps
    UNIT_ASSERT_VALUES_EQUAL(seenPlanSteps, 0u);
}

Y_UNIT_TEST(TestImmediateQueueThenSplit) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    NKikimrConfig::TAppConfig app;
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto tablets = GetTableShards(server, sender, "/Root/table-1");

    // We need shard to have some data (otherwise it would die too quickly)
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (0, 0);"));

    bool captureSplit = true;
    bool captureSplitChanged = true;
    bool capturePropose = true;
    THashSet<TActorId> captureDelayedProposeFrom;
    TVector<THolder<IEventHandle>> eventsSplit;
    TVector<THolder<IEventHandle>> eventsSplitChanged;
    TVector<THolder<IEventHandle>> eventsPropose;
    TVector<THolder<IEventHandle>> eventsDelayedPropose;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &event) -> auto {
        switch (event->GetTypeRewrite()) {
            case TEvDataShard::EvSplit:
                if (captureSplit) {
                    Cerr << "---- captured EvSplit ----" << Endl;
                    eventsSplit.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case TEvDataShard::EvSplitPartitioningChanged:
                if (captureSplitChanged) {
                    Cerr << "---- captured EvSplitPartitioningChanged ----" << Endl;
                    eventsSplitChanged.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case TEvDataShard::EvProposeTransaction:
            case NKikimr::NEvents::TDataEvents::EvWrite:
                if (capturePropose) {
                    Cerr << "---- capture EvProposeTransaction ----" << Endl;
                    eventsPropose.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 2 /* EvDelayedProposeTransaction */:
                if (captureDelayedProposeFrom.contains(event->GetRecipientRewrite())) {
                    Cerr << "---- capture EvDelayedProposeTransaction ----" << Endl;
                    Cerr << event->GetTypeName() << Endl;
                    eventsDelayedPropose.emplace_back(event.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    const int totalWrites = 10;
    TVector<TActorId> writeSenders;

    // Send a lot of write requests in parallel (so there's a large propose queue)
    for (int i = 0; i < totalWrites; ++i) {
        auto writeSender = runtime.AllocateEdgeActor();
        SendSQL(server, writeSender, Q_(Sprintf("UPSERT INTO `/Root/table-1` (key, value) VALUES (%d, %d);", i, i)));
        writeSenders.push_back(writeSender);
    }

    // Split would fail otherwise :(
    SetSplitMergePartCountLimit(server->GetRuntime(), -1);

    // Start split for table-1
    TInstant splitStart = TInstant::Now();
    auto senderSplit = runtime.AllocateEdgeActor();
    ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", tablets[0], 100);

    // Wait until all propose requests and split reach our shard
    if (!(eventsSplit.size() >= 1 && eventsPropose.size() >= totalWrites)) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle&) -> bool {
                return eventsSplit.size() >= 1 && eventsPropose.size() >= totalWrites;
            });
        runtime.DispatchEvents(options);
    }

    // Unblock all propose requests (so they all successfully pass state test)
    capturePropose = false;
    for (auto& ev : eventsPropose) {
        Cerr << "---- Unblocking propose transaction ----" << Endl;
        captureDelayedProposeFrom.insert(ev->GetRecipientRewrite());
        runtime.Send(ev.Release(), 0, true);
    }
    eventsPropose.clear();

    // Unblock split request (shard will move to SplitSrcWaitForNoTxInFlight)
    captureSplit = false;
    captureSplitChanged = true;
    for (auto& ev : eventsSplit) {
        Cerr << "---- Unblocking split ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsSplit.clear();

    // Wait until split is finished and we have a delayed propose
    Cerr << "---- Waiting for split to finish ----" << Endl;
    if (!(eventsSplitChanged.size() >= 1 && eventsDelayedPropose.size() >= 1)) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            [&](IEventHandle&) -> bool {
                return eventsSplitChanged.size() >= 1 && eventsDelayedPropose.size() >= 1;
            });
        runtime.DispatchEvents(options);
    }

    // Cause split to finish before the first delayed propose
    captureSplitChanged = false;
    for (auto& ev : eventsSplitChanged) {
        Cerr << "---- Unblocking split finish ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsSplitChanged.clear();

    // Unblock delayed propose transactions
    captureDelayedProposeFrom.clear();
    for (auto& ev : eventsDelayedPropose) {
        Cerr << "---- Unblocking delayed propose ----" << Endl;
        runtime.Send(ev.Release(), 0, true);
    }
    eventsDelayedPropose.clear();

    // Wait for split to finish at schemeshard
    WaitTxNotification(server, senderSplit, txId);

    // Split shouldn't take too much time to complete
    TDuration elapsed = TInstant::Now() - splitStart;
    UNIT_ASSERT_C(elapsed < TDuration::Seconds(NValgrind::PlainOrUnderValgrind(5, 25)),
        "Split needed " << elapsed.ToString() << " to complete, which is too long");

    // Count transaction results
    int successes = 0;
    int failures = 0;
    for (auto writeSender : writeSenders) {
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(writeSender);
        if (ev->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            ++successes;
        } else {
            ++failures;
        }
    }

    // We expect all transactions to fail
    UNIT_ASSERT_C(successes + failures == totalWrites,
        "Unexpected "
        << successes << " successes and "
        << failures << " failures");
}

void TestLateKqpQueryAfterColumnDrop(bool dataQuery, const TString& query) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    app.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(false);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();
    auto streamSender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_PROXY, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_WORKER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_TASKS_RUNNER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_COMPUTE, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::KQP_RESOURCE_MANAGER, NLog::PRI_DEBUG);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value1", "Uint32", false, false},
                {"value2", "Uint32", false, false}}));

    ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value1, value2) VALUES (1, 1, 10), (2, 2, 20);");

    bool capturePropose = true;
    TVector<THolder<IEventHandle>> eventsPropose;
    auto captureEvents = [&](TAutoPtr<IEventHandle> &ev) -> auto {
        // if (ev->GetRecipientRewrite() == streamSender) {
        //     Cerr << "Stream sender got " << ev->GetTypeRewrite() << " " << ev->GetBase()->ToStringHeader() << Endl;
        // }
        switch (ev->GetTypeRewrite()) {
            case TEvDataShard::EvProposeTransaction: {
                auto &rec = ev->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (capturePropose && rec.GetTxKind() != NKikimrTxDataShard::TX_KIND_SNAPSHOT) {
                    Cerr << "---- capture EvProposeTransaction ---- type=" << rec.GetTxKind() << Endl;
                    eventsPropose.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }

            case NKikimr::NEvents::TDataEvents::EvWrite: {
                if (capturePropose) {
                    Cerr << "---- capture EvWrite ----" << Endl;
                    eventsPropose.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }

            case TEvDataShard::EvKqpScan: {
                if (capturePropose) {
                    Cerr << "---- capture EvKqpScan ----" << Endl;
                    eventsPropose.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            default:
                break;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureEvents);

    std::function<void()> processEvents = [&]() {
        // Wait until there's exactly one propose message at our datashard
        if (eventsPropose.size() < 1) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return eventsPropose.size() >= 1;
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(eventsPropose.size(), 1u);
        Cerr << "--- captured scan tx proposal" << Endl;
        capturePropose = false;

        // Drop column value2 and wait for drop to finish
        auto dropTxId = AsyncAlterDropColumn(server, "/Root", "table-1", "value2");
        WaitTxNotification(server, dropTxId);

        // Resend delayed propose messages
        Cerr << "--- resending captured proposals" << Endl;
        for (auto& ev : eventsPropose) {
            runtime.Send(ev.Release(), 0, true);
        }
        eventsPropose.clear();
        return;
    };

    if (dataQuery) {
        Cerr << "--- sending data query request" << Endl;
        auto tmp = CreateSessionRPC(runtime);
        auto f = SendRequest(runtime, MakeSimpleRequestRPC(query, tmp, "", true));
        processEvents();
        auto response = AwaitResponse(runtime, f);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::ABORTED);
    } else {
        Cerr << "--- sending stream request" << Endl;
        SendRequest(runtime, streamSender, MakeStreamRequest(streamSender, query, false));
        processEvents();

        Cerr << "--- waiting for result" << Endl;
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(streamSender);
        auto& response = ev->Get()->Record;
        Cerr << response.DebugString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(response.GetYdbStatus(), Ydb::StatusIds::ABORTED);
        auto& issue = response.GetResponse().GetQueryIssues(0);
        UNIT_ASSERT_VALUES_EQUAL(issue.issue_code(), (int) NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH);
        UNIT_ASSERT_STRINGS_EQUAL(issue.message(), "Table \'/Root/table-1\' scheme changed.");
    }
}

Y_UNIT_TEST(TestLateKqpScanAfterColumnDrop) {
    TestLateKqpQueryAfterColumnDrop(false, "SELECT SUM(value2) FROM `/Root/table-1`");
}

Y_UNIT_TEST(TestSecondaryClearanceAfterShardRestartRace) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::KQP_EXECUTER, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 2);
    auto shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);"));

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // We want to intercept delivery problem notifications
    TVector<THolder<IEventHandle>> capturedDeliveryProblem;
    size_t seenStreamClearanceRequests = 0;
    size_t seenStreamClearanceResponses = 0;
    auto captureEvents = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvPipeCache::TEvDeliveryProblem::EventType: {
                Cerr << "... intercepted TEvDeliveryProblem" << Endl;
                capturedDeliveryProblem.emplace_back(ev.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvTxProcessing::TEvStreamQuotaRelease::EventType: {
                Cerr << "... dropped TEvStreamQuotaRelease" << Endl;
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvTxProcessing::TEvStreamClearanceRequest::EventType: {
                Cerr << "... observed TEvStreamClearanceRequest" << Endl;
                ++seenStreamClearanceRequests;
                break;
            }
            case TEvTxProcessing::TEvStreamClearanceResponse::EventType: {
                Cerr << "... observed TEvStreamClearanceResponse" << Endl;
                ++seenStreamClearanceResponses;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(captureEvents);

    auto state = StartReadShardedTable(server, "/Root/table-1", TRowVersion::Max(), /* pause */ true, /* ordered */ false);

    waitFor([&]{ return seenStreamClearanceResponses >= 2; }, "observed TEvStreamClearanceResponse");

    seenStreamClearanceRequests = 0;
    seenStreamClearanceResponses = 0;
    RebootTablet(runtime, shards[0], sender);

    waitFor([&]{ return capturedDeliveryProblem.size() >= 1; }, "intercepted TEvDeliveryProblem");
    waitFor([&]{ return seenStreamClearanceRequests >= 1; }, "observed TEvStreamClearanceRequest");

    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : capturedDeliveryProblem) {
        runtime.Send(ev.Release(), 0, true);
    }

    ResumeReadShardedTable(server, state);

    // We expect this upsert to complete successfully
    // When there's a bug it will get stuck due to readtable before barrier
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4);"));
}

Y_UNIT_TEST(TestShardRestartNoUndeterminedImmediate) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
        .Indexes({
            // Note: async index forces read before write at the shard level,
            // which causes immediate upsert to block until volatile transaction
            // is resolved.
            TShardedTableOptions::TIndex{
                "by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync
            }
        }));
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1);"));

    TString sessionId = CreateSessionRPC(runtime);
    TString sender3Session = CreateSessionRPC(runtime);

    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    size_t delayedProposeCount = 0;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
            case EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 2 /* EvDelayedProposeTransaction */: {
                ++delayedProposeCount;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto myCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    waitFor([&]{ return readSets.size() >= expectedReadSets; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Now send an upsert to table-1, it should be blocked by our in-progress tx
    delayedProposeCount = 0;
    Cerr << "... sending immediate upsert" << Endl;
    auto f3 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 42), (3, 51))"), sender3Session, "", true));

    // Wait unti that propose starts to execute
    waitFor([&]{ return delayedProposeCount >= 1; }, "immediate propose");
    UNIT_ASSERT_VALUES_EQUAL(delayedProposeCount, 1u);
    Cerr << "... immediate upsert is blocked" << Endl;

    // Remove observer and gracefully restart the shard
    runtime.SetObserverFunc(prevObserverFunc);
    GracefulRestartTablet(runtime, table1shards[0], sender);

    // The result of immediate upsert must be neither SUCCESS nor UNDETERMINED
    {
        auto response = AwaitResponse(runtime, f3);
        UNIT_ASSERT_VALUES_UNEQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_UNEQUAL(response.operation().status(), Ydb::StatusIds::UNDETERMINED);
    }

    // Select key 1 and verify its value was not updated
    {
        auto result = KqpSimpleExec(runtime, Q_(R"(SELECT key, value FROM `/Root/table-1` WHERE key = 1 ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result, "{ items { uint32_value: 1 } items { uint32_value: 1 } }");
    }
}

Y_UNIT_TEST(TestShardRestartPlannedCommitShouldSucceed) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app)
        // Note: currently volatile transactions don't survive tablet reboots,
        // and reply with UNDETERMINED similar to immediate transactions.
        .SetEnableDataShardVolatileTransactions(false)
        .SetEnableDataShardWriteAlwaysVolatile(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    const bool usesVolatileTxs = runtime.GetAppData(0).FeatureFlags.GetEnableDataShardVolatileTransactions();

    InitRoot(server, sender);

    auto [shards1, tableId1] = CreateShardedTable(server, sender, "/Root", "table-1", 1);
    auto [shards2, tableId2] = CreateShardedTable(server, sender, "/Root", "table-2", 1);

    {
        Cerr << "===== UPSERT initial rows" << Endl;

        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
        ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 1)"));
    }

    TString sessionId = CreateSessionRPC(runtime);

    TString txId;
    {
        Cerr << "===== Begin SELECT" << Endl;

        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 1 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    Cerr << "===== UPSERT and commit" << Endl;

    // Send a commit request, it would block on readset exchange
    auto myCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 2);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 2))"), sessionId, txId, true));

    // Wait until we captured both readsets
    const size_t expectedReadSets = usesVolatileTxs ? 4 : 2;
    waitFor([&]{ return readSets.size() >= expectedReadSets; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), expectedReadSets);

    // Remove observer and gracefully restart the shard
    Cerr << "===== restarting tablet" << Endl;
    runtime.SetObserverFunc(prevObserverFunc);
    GracefulRestartTablet(runtime, shards1[0], sender);

    // The result of commit should be SUCCESS
    {
        Cerr << "===== Waiting for commit response" << Endl;

        auto response = AwaitResponse(runtime, myCommit2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Select key 3 and verify its value was updated
    {
        Cerr << "===== Last SELECT" << Endl;

        auto result = KqpSimpleExec(runtime, Q_(R"(SELECT key, value FROM `/Root/table-1` WHERE key = 3 ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(result,  "{ items { uint32_value: 3 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestShardRestartDuringWaitingRead) {
    TPortManager pm;
    NKikimrConfig::TAppConfig app;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app)
        // We read from an unresolved volatile tx
        .SetEnableDataShardVolatileTransactions(true);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 20)"));

    // Block readset exchange
    std::vector<std::unique_ptr<IEventHandle>> readSets;
    auto blockReadSets = runtime.AddObserver<TEvTxProcessing::TEvReadSet>([&](TEvTxProcessing::TEvReadSet::TPtr& ev) {
        readSets.emplace_back(ev.Release());
    });

    // Start a distributed write to both tables
    TString sessionId = CreateSessionRPC(runtime, "/Root");
    auto upsertResult = SendRequest(
        runtime,
        MakeSimpleRequestRPC(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 30);
            UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 40);
            )", sessionId, /* txId */ "", /* commitTx */ true),
        "/Root");
    WaitFor(runtime, [&]{ return readSets.size() >= 4; }, "readsets");

    // Start reading the first table
    TString readSessionId = CreateSessionRPC(runtime, "/Root");
    auto readResult = SendRequest(
        runtime,
        MakeSimpleRequestRPC(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key;
            )", readSessionId, /* txId */ "", /* commitTx */ true),
        "/Root");

    // Sleep to ensure read is properly waiting
    runtime.SimulateSleep(TDuration::Seconds(1));

    // Gracefully restart the first table shard
    blockReadSets.Remove();
    GracefulRestartTablet(runtime, table1shards[0], sender);

    // Read succeeds because it is automatically retried
    // No assert should be triggered in debug builds
    UNIT_ASSERT_VALUES_EQUAL(
        FormatResult(AwaitResponse(runtime, std::move(readResult))),
        "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 30 } }");
}

Y_UNIT_TEST(TestShardSnapshotReadNoEarlyReply) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    // runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NLog::PRI_TRACE);
    // runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));
    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");
    auto isTableShard = [&](ui64 tabletId) -> bool {
        if (std::find(table1shards.begin(), table1shards.end(), tabletId) != table1shards.end() ||
            std::find(table2shards.begin(), table2shards.end(), tabletId) != table2shards.end())
        {
            return true;
        }
        return false;
    };

    SimulateSleep(server, TDuration::Seconds(1));

    auto waitFor = [&](const auto& condition, const TString& description) {
        for (int i = 0; i < 5; ++i) {
            if (condition()) {
                return;
            }
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
        UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
    };

    TVector<THolder<IEventHandle>> blockedCommits;
    size_t seenProposeResults = 0;
    auto blockCommits = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTablet::TEvCommit::EventType: {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (isTableShard(msg->TabletID)) {
                    Cerr << "... blocked commit for tablet " << msg->TabletID << Endl;
                    blockedCommits.push_back(std::move(ev));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::TEvProposeTransactionResult::EventType: {
                Cerr << "... observed propose transaction result" << Endl;
                ++seenProposeResults;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(blockCommits);

    TString sessionId1 = CreateSessionRPC(runtime, "/Root");
    TString sessionId2 = CreateSessionRPC(runtime, "/Root");

    TString sq1 = CreateSessionRPC(runtime, "/Root");
    TString sq2 = CreateSessionRPC(runtime, "/Root");

    auto f1 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId1, "", false), "/Root");
    auto f2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId2, "", false), "/Root");

    waitFor([&]{ return blockedCommits.size() >= 2; }, "at least 2 blocked commits");

    SimulateSleep(server, TDuration::Seconds(1));

    UNIT_ASSERT_C(seenProposeResults == 0, "Unexpected propose results observed");

    // Unblock commits and wait for results
    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : blockedCommits) {
        runtime.Send(ev.Release(), 0, true);
    }
    blockedCommits.clear();

    TString txId1;
    {
        auto response = AwaitResponse(runtime, f1);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId1 = result.tx_meta().id();
    }

    TString txId2;
    {
        auto response = AwaitResponse(runtime, f2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        txId2 = result.tx_meta().id();
    }

    // Perform a couple of immediate reads to make sure shards are ready to respond to read-only requests
    // This may be needed when leases are enabled, otherwise paused leases would block the snapshot read reply
    ExecSQL(server, sender, Q_("SELECT * FROM `/Root/table-1` WHERE key = 1"));
    ExecSQL(server, sender, Q_("SELECT * FROM `/Root/table-2` WHERE key = 2"));
    Cerr << "... shards are ready for read-only immediate transactions" << Endl;

    // Start blocking commits again and try performing new writes
    prevObserver = runtime.SetObserverFunc(blockCommits);
    SendRequest(runtime, MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"), sq1, "", true), "/Root");
    SendRequest(runtime, MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (4, 4)"), sq2, "", true), "/Root");
    waitFor([&]{ return blockedCommits.size() >= 2; }, "at least 2 blocked commits");

    // Send an additional read request, it must not be blocked
    auto f = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        SELECT * FROM `/Root/table-1`
        UNION ALL
        SELECT * FROM `/Root/table-2`
    )"), sessionId1, txId1, false), "/Root");

    {
        auto response = AwaitResponse(runtime, f);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
}

Y_UNIT_TEST(TestSnapshotReadAfterBrokenLock) {
    NKikimrConfig::TAppConfig app;
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    TString sessionId = CreateSessionRPC(runtime);

    // Start transaction by reading from both tables, we will only set locks
    // to currently existing variables
    TString txId;
    {
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    SimulateSleep(server, TDuration::Seconds(1));

    // Perform immediate write, which would not break the above lock
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    // Perform an additional read, it would mark transaction as write-broken
    {
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 3
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    // Perform one more read, it would be in an already write-broken transaction
    {
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 5
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    {
        auto result = KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
    }
}

Y_UNIT_TEST(TestSnapshotReadAfterBrokenLockOutOfOrder) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Start transaction by reading from both tables
    TString sessionId = CreateSessionRPC(runtime);
    TString txId;
    {
        Cerr << "... performing the first select" << Endl;
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    // Arrange for another distributed tx stuck at readset exchange
    TString sessionIdBlocker = CreateSessionRPC(runtime);
    TString txIdBlocker;
    {
        auto result = KqpSimpleBegin(runtime, sessionIdBlocker, txIdBlocker, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(std::move(ev));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto thisCommit2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99))"), sessionIdBlocker, txIdBlocker, true));

    // Wait until we captured both readsets
    waitFor([&]{ return readSets.size() >= 2; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), 2u);

    // Restore the observer, we would no longer block new readsets
    runtime.SetObserverFunc(prevObserverFunc);

    SimulateSleep(server, TDuration::Seconds(1));

    // Perform immediate write, which would break the above lock
    Cerr << "... performing an upsert" << Endl;
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 3)"));

    // Perform an additional read, it would mark transaction as write-broken for the first time
    {
        Cerr << "... performing the second select" << Endl;
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 3
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    // Perform one more read, it would be in an already write-broken transaction
    {
        Cerr << "... performing the third select" << Endl;
        auto result = KqpSimpleContinue(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 5
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "");
    }

    {
        Cerr << "... performing the last upsert and commit" << Endl;
        auto result = KqpSimpleCommit(runtime, sessionId, txId, Q_(R"(
            UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"));
        UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ABORTED");
    }
}

Y_UNIT_TEST(TestSnapshotReadAfterStuckRW) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Arrange for a distributed tx stuck at readset exchange
    TString sessionIdBlocker = CreateSessionRPC(runtime);
    TString txIdBlocker;
    {
        auto result = KqpSimpleBegin(runtime, sessionIdBlocker, txIdBlocker, Q_(R"(
            SELECT * FROM `/Root/table-1`
            UNION ALL
            SELECT * FROM `/Root/table-2`
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }

    auto waitFor = [&](const auto& condition, const TString& description) {
        if (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
            UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
        }
    };

    // Capture and block all readset messages
    TVector<THolder<IEventHandle>> readSets;
    auto captureRS = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvReadSet::EventType: {
                Cerr << "... captured readset" << Endl;
                readSets.push_back(THolder(ev.Release()));
                return TTestActorRuntime::EEventAction::DROP;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureRS);

    // Send a commit request, it would block on readset exchange
    auto sCommit = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (99, 99);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (99, 99))"), sessionIdBlocker, txIdBlocker, true));

    // Wait until we captured both readsets
    waitFor([&]{ return readSets.size() >= 2; }, "commit read sets");
    UNIT_ASSERT_VALUES_EQUAL(readSets.size(), 2u);

    // Restore the observer, we would no longer block new readsets
    runtime.SetObserverFunc(prevObserverFunc);

    SimulateSleep(server, TDuration::Seconds(1));

    // Start a transaction by reading from both tables
    TString sessionId = CreateSessionRPC(runtime);
    TString txId;
    {
        Cerr << "... performing the first select" << Endl;
        auto result = KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT * FROM `/Root/table-1` WHERE key = 1
            UNION ALL
            SELECT * FROM `/Root/table-2` WHERE key = 2
            ORDER BY key)"));
        UNIT_ASSERT_VALUES_EQUAL(
            result,
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
    }
}

Y_UNIT_TEST(TestSnapshotReadPriority) {
    TPortManager pm;
    TServerSettings::TControls controls;
    // This test needs to make sure mediator time does not advance while
    // certain operations are running. Unfortunately, volatile planning
    // may happen every 1ms, and it's too hard to guarantee time stays
    // still for such a short time. We disable volatile planning to make
    // coordinator ticks are 100ms apart.
    controls.MutableCoordinatorControls()->SetVolatilePlanLeaseMs(0);
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    TString senderImmediateWrite = CreateSessionRPC(runtime);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));

    SimulateSleep(server, TDuration::MilliSeconds(850));

    // Perform an immediate write
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    auto execSimpleRequest = [&](const TString& query) -> TString {
        return KqpSimpleExec(runtime, query);
    };

    auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
        return KqpSimpleContinue(runtime, sessionId, txId, query);
    };

    auto execSnapshotRequest = [&](const TString& query) -> TString {
        TString sessionId, txId;
        TString result = beginSnapshotRequest(sessionId, txId, query);
        CloseSession(runtime, sessionId);
        return result;
    };

    // Perform an immediate read, we should observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Send an immediate write transaction, but don't wait for result
    auto fImmWrite = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (5, 5)
        )"), senderImmediateWrite, "", true));

    // We sleep for very little so datashard commits changes, but doesn't advance
    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Perform an immediate read again, it should NOT observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }");

    // Wait for the write to finish
    {
        auto response = AwaitResponse(runtime, fImmWrite);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // Perform an immediate read again, it should observe the write above
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Same when using a fresh snapshot read
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Start a new write and sleep again
    fImmWrite = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (7, 7)
        )"), senderImmediateWrite, "", true));

    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Verify this write is not observed yet
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Reboot the tablet
    RebootTablet(runtime, table1shards.at(0), sender);

    // Verify the write above cannot be observed after restart as well
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Send one more write and sleep again
    auto senderImmediateWrite2 = CreateSessionRPC(runtime);
    auto fImmWrite2 = SendRequest(runtime, MakeSimpleRequestRPC(Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (9, 9)
        )"), senderImmediateWrite2, "", true));

    SimulateSleep(runtime, TDuration::MicroSeconds(1));

    // Verify it is also hidden at the moment
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }");

    // Wait for result of the second write
    {
        auto response = AwaitResponse(runtime, fImmWrite2);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // We should finally observe both writes
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    TString snapshotSessionId, snapshotTxId;
    UNIT_ASSERT_VALUES_EQUAL(
        beginSnapshotRequest(snapshotSessionId, snapshotTxId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    // Spam schedules in the runtime to prevent mediator time jumping prematurely
    {
        Cerr << "!!! Setting up wakeup spam" << Endl;
        auto senderWakeupSpam = runtime.AllocateEdgeActor();
        for (int i = 1; i <= 10; ++i) {
            runtime.Schedule(new IEventHandle(senderWakeupSpam, senderWakeupSpam, new TEvents::TEvWakeup()), TDuration::MicroSeconds(i * 250));
        }
    }

    // Reboot the tablet
    RebootTablet(runtime, table1shards.at(0), sender);

    // Upsert new data after reboot
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (11, 11)"));

    // Make sure datashard state is restored correctly and snapshot is not corrupted
    UNIT_ASSERT_VALUES_EQUAL(
        continueSnapshotRequest(snapshotSessionId, snapshotTxId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }");

    // Make sure new snapshot will actually observe new data
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
        "{ items { uint32_value: 5 } items { uint32_value: 5 } }, "
        "{ items { uint32_value: 7 } items { uint32_value: 7 } }, "
        "{ items { uint32_value: 9 } items { uint32_value: 9 } }, "
        "{ items { uint32_value: 11 } items { uint32_value: 11 } }");
}

Y_UNIT_TEST(TestUnprotectedReadsThenWriteVisibility) {
    TPortManager pm;
    TServerSettings::TControls controls;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetNodeCount(2)
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    const ui64 hiveTabletId = ChangeStateStorage(Hive, server->GetSettings().Domain);

    struct TNodeState {
        // mediator -> bucket -> [observed, passed] step
        THashMap<ui64, THashMap<ui32, std::pair<ui64, ui64>>> Steps;
        ui64 AllowedStep = 0;
    };
    THashMap<ui32, TNodeState> mediatorState;

    // Don't allow granular timecast side-stepping mediator time hacks in this test
    TBlockEvents<TEvMediatorTimecast::TEvGranularUpdate> blockGranularUpdate(runtime);

    bool mustWaitForSteps[2] = { false, false };

    auto captureTimecast = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        const ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        switch (ev->GetTypeRewrite()) {
            case TEvMediatorTimecast::TEvUpdate::EventType: {
                auto* msg = ev->Get<TEvMediatorTimecast::TEvUpdate>();
                const ui64 mediatorId = msg->Record.GetMediator();
                const ui32 bucket = msg->Record.GetBucket();
                ui64 step = msg->Record.GetTimeBarrier();
                auto& state = mediatorState[nodeId];
                if (!mustWaitForSteps[nodeIndex]) {
                    // Automatically allow all new steps
                    state.AllowedStep = Max(state.AllowedStep, step);
                }
                Cerr << "... node " << nodeId << " observed update from " << mediatorId
                    << " for bucket " << bucket
                    << " to step " << step
                    << " (allowed " << state.AllowedStep << ")"
                    << Endl;
                auto& [observedStep, passedStep] = state.Steps[mediatorId][bucket];
                observedStep = Max(observedStep, step);
                if (step >= passedStep) {
                    if (step < state.AllowedStep) {
                        step = state.AllowedStep;
                        msg->Record.SetTimeBarrier(step);
                        Cerr << "...     shifted to allowed step " << step << Endl;
                    }
                    passedStep = step;
                    break;
                }
                return TTestActorRuntime::EEventAction::DROP;
            }
            case TEvMediatorTimecast::TEvWaitPlanStep::EventType: {
                const auto* msg = ev->Get<TEvMediatorTimecast::TEvWaitPlanStep>();
                const ui64 tabletId = msg->TabletId;
                const ui64 step = msg->PlanStep;
                Cerr << "... node " << nodeId << " observed wait by " << tabletId
                    << " for step " << step
                    << Endl;
                auto& state = mediatorState[nodeId];
                if (state.AllowedStep < step) {
                    state.AllowedStep = step;
                    for (auto& kv1 : state.Steps) {
                        const ui64 mediatorId = kv1.first;
                        for (auto& kv2 : kv1.second) {
                            const ui32 bucket = kv2.first;
                            auto& [observedStep, passedStep] = kv2.second;
                            if (passedStep < step && passedStep < observedStep) {
                                passedStep = Min(step, observedStep);
                                auto* update = new TEvMediatorTimecast::TEvUpdate();
                                update->Record.SetMediator(mediatorId);
                                update->Record.SetBucket(bucket);
                                update->Record.SetTimeBarrier(passedStep);
                                runtime.Send(new IEventHandle(ev->GetRecipientRewrite(), ev->GetRecipientRewrite(), update), nodeIndex, /* viaActorSystem */ true);
                            }
                        }
                    }
                }
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureTimecast);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");

    // Make sure tablet is at node 1
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(0)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

    SimulateSleep(server, TDuration::Seconds(1));

    auto execSimpleRequest = [&](const TString& query) -> TString {
        return KqpSimpleExec(runtime, query);
    };

    auto beginSnapshotRequest = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    auto continueSnapshotRequest = [&](const TString& sessionId, const TString& txId, const TString& query) -> TString {
        return KqpSimpleContinue(runtime, sessionId, txId, query);
    };

    auto execSnapshotRequest = [&](const TString& query) -> TString {
        TString sessionId, txId;
        TString result = beginSnapshotRequest(sessionId, txId, query);
        CloseSession(runtime, sessionId);
        return result;
    };

    // Perform an immediate read, we should observe the initial write
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Same when using a fresh snapshot read
    TString sessionId, txId;
    UNIT_ASSERT_VALUES_EQUAL(
        beginSnapshotRequest(sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Stop updating mediator timecast on the second node
    mustWaitForSteps[1] = true;

    // Insert a new row and wait for result
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));

    // Make sure tablet is at node 2
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(1)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // Perform an immediate read, we should observe confirmed writes after restart
    UNIT_ASSERT_VALUES_EQUAL(
        execSimpleRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

    // Previous snapshot must see original data
    UNIT_ASSERT_VALUES_EQUAL(
        continueSnapshotRequest(sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // However new snapshots must see updated data
    UNIT_ASSERT_VALUES_EQUAL(
        execSnapshotRequest(Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");
}

Y_UNIT_TEST(UncommittedReadSetAck) {
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetNodeCount(2)
        .SetUseRealThreads(false);


    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    const ui64 hiveTabletId = ChangeStateStorage(Hive, server->GetSettings().Domain);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);
    CreateShardedTable(server, sender, "/Root", "table-2", 1);

    auto table1shards = GetTableShards(server, sender, "/Root/table-1");
    auto table2shards = GetTableShards(server, sender, "/Root/table-2");

    // Make sure these tablets are at node 1
    runtime.SendToPipe(hiveTabletId, sender, new TEvHive::TEvFillNode(runtime.GetNodeId(0)));
    {
        auto ev = runtime.GrabEdgeEventRethrow<TEvHive::TEvFillNodeResult>(sender);
        UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrProto::OK);
    }

    // Create one more table, we expect it to run at node 2
    CreateShardedTable(server, sender, "/Root", "table-3", 1);
    auto table3shards = GetTableShards(server, sender, "/Root/table-3");
    auto table3actor = ResolveTablet(runtime, table3shards.at(0), /* nodeIndex */ 1);
    UNIT_ASSERT_VALUES_EQUAL(table3actor.NodeId(), runtime.GetNodeId(1));

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-3` (key, value) VALUES (3, 3)"));

    auto beginTx = [&](TString& sessionId, TString& txId, const TString& query) -> TString {
        return KqpSimpleBegin(runtime, sessionId, txId, query);
    };

    TString sessionId1, txId1;
    UNIT_ASSERT_VALUES_EQUAL(
        beginTx(sessionId1, txId1, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    TString sessionId2, txId2;
    UNIT_ASSERT_VALUES_EQUAL(
        beginTx(sessionId2, txId2, Q_(R"(
            SELECT key, value FROM `/Root/table-2`
            ORDER BY key
            )")),
        "{ items { uint32_value: 2 } items { uint32_value: 2 } }");

    bool capturePlanSteps = true;
    TVector<THolder<IEventHandle>> capturedPlanSteps;
    TVector<ui64> capturedPlanTxIds;
    THashSet<ui64> passReadSetTxIds;
    ui64 observedReadSets = 0;
    TVector<THolder<IEventHandle>> capturedReadSets;
    ui64 observedReadSetAcks = 0;
    bool captureCommits = false;
    TVector<THolder<IEventHandle>> capturedCommits;

    auto captureCommitAfterReadSet = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        const ui32 nodeId = ev->GetRecipientRewrite().NodeId();
        const ui32 nodeIndex = nodeId - runtime.GetNodeId(0);
        if (nodeIndex == 1) {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProcessing::TEvPlanStep::EventType: {
                if (nodeIndex == 1 && ev->GetRecipientRewrite() == table3actor && capturePlanSteps) {
                    Cerr << "... captured plan step for table-3" << Endl;
                    auto* msg = ev->Get<TEvTxProcessing::TEvPlanStep>();
                    for (const auto& tx : msg->Record.GetTransactions()) {
                        ui64 txId = tx.GetTxId();
                        capturedPlanTxIds.push_back(txId);
                        Cerr << "... captured plan step tx " << txId << " for table-3" << Endl;
                    }
                    capturedPlanSteps.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvTxProcessing::TEvReadSet::EventType: {
                if (nodeIndex == 1 && ev->GetRecipientRewrite() == table3actor) {
                    auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    ui64 txId = msg->Record.GetTxId();
                    if ((msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET) &&
                        (msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA))
                    {
                        Cerr << "... passing expectation for txid# " << txId << Endl;
                        break;
                    }
                    ++observedReadSets;
                    if (!passReadSetTxIds.contains(txId)) {
                        Cerr << "... readset for txid# " << txId << " was blocked" << Endl;
                        capturedReadSets.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... passing readset for txid# " << txId << Endl;
                }
                break;
            }
            case TEvTxProcessing::TEvReadSetAck::EventType: {
                Cerr << "... read set ack" << Endl;
                ++observedReadSetAcks;
                break;
            }
            case TEvBlobStorage::TEvPut::EventType: {
                auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                if (nodeIndex == 1 && msg->Id.TabletID() == table3shards.at(0) && captureCommits) {
                    Cerr << "... capturing put " << msg->Id << " for table-3" << Endl;
                    capturedCommits.emplace_back(ev.Release());
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
        }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserverFunc = runtime.SetObserverFunc(captureCommitAfterReadSet);

    // Make two commits in parallel, one of them will receive a readset and become complete
    SendRequest(runtime, MakeSimpleRequestRPC(
        Q_(R"(
            UPSERT INTO `/Root/table-3` (key, value) VALUES (4, 4)
        )"), sessionId1, txId1, true));
    SendRequest(runtime, MakeSimpleRequestRPC(
        Q_(R"(
            UPSERT INTO `/Root/table-3` (key, value) VALUES (5, 5)
        )"), sessionId2, txId2, true));

    auto waitFor = [&](const auto& condition, const TString& description) {
        while (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
    };

    waitFor([&]{ return capturedPlanTxIds.size() >= 2; }, "captured transactions");
    UNIT_ASSERT_C(capturedPlanTxIds.size(), 2u);
    std::sort(capturedPlanTxIds.begin(), capturedPlanTxIds.end());
    ui64 realTxId1 = capturedPlanTxIds.at(0);
    ui64 realTxId2 = capturedPlanTxIds.at(1);

    // Unblock and resend the plan step message
    capturePlanSteps = false;
    for (auto& ev : capturedPlanSteps) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedPlanSteps.clear();

    // Wait until there are 2 readset messages (with data)
    waitFor([&]{ return capturedReadSets.size() >= 2; }, "initial readsets");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Unblock readset messages for txId1, but block commits
    captureCommits = true;
    observedReadSetAcks = 0;
    passReadSetTxIds.insert(realTxId1);
    for (auto& ev : capturedReadSets) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedReadSets.clear();

    // Wait until transaction is complete and tries to commit
    waitFor([&]{ return capturedCommits.size() > 0 && capturedReadSets.size() > 0; }, "tx complete");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Reboot tablets and wait for resent readsets
    // Since tx is already complete, it will be added to DelayedAcks
    observedReadSets = 0;
    capturedReadSets.clear();
    RebootTablet(runtime, table1shards.at(0), sender, 0, true);
    RebootTablet(runtime, table2shards.at(0), sender, 0, true);

    waitFor([&]{ return observedReadSets >= 2; }, "resent readsets");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // Now we unblock the second readset and resend it
    passReadSetTxIds.insert(realTxId2);
    for (auto& ev : capturedReadSets) {
        runtime.Send(ev.Release(), 1, true);
    }
    capturedReadSets.clear();
    observedReadSets = 0;

    // Wait until the second transaction commits
    ui64 prevCommitsBlocked = capturedCommits.size();
    waitFor([&]{ return capturedCommits.size() > prevCommitsBlocked && observedReadSets >= 1; }, "second tx complete");
    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // There must be no readset acks, since we're blocking all commits
    UNIT_ASSERT_VALUES_EQUAL(observedReadSetAcks, 0);

    // Now we stop blocking anything and "reply" to all blocked commits with an error
    runtime.SetObserverFunc(prevObserverFunc);
    for (auto& ev : capturedCommits) {
        auto proxy = ev->Recipient;
        ui32 groupId = GroupIDFromBlobStorageProxyID(proxy);
        auto res = ev->Get<TEvBlobStorage::TEvPut>()->MakeErrorResponse(NKikimrProto::ERROR, "Something went wrong", TGroupId::FromValue(groupId));
        runtime.Send(new IEventHandle(ev->Sender, proxy, res.release()), 1, true);
    }
    capturedCommits.clear();

    SimulateSleep(runtime, TDuration::MilliSeconds(5));

    // This should succeed, unless the bug was triggered and readset acknowledged before commit
    ExecSQL(server, sender, Q_(R"(
        UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 42);
        UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 42);
        UPSERT INTO `/Root/table-3` (key, value) VALUES (4, 42), (5, 42);
    )"));
}

Y_UNIT_TEST(UncommittedReads) {
    TPortManager pm;
    TServerSettings::TControls controls;

    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetControls(controls)
        .SetUseRealThreads(false);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TDisableDataShardLogBatching disableDataShardLogBatching;
    CreateShardedTable(server, sender, "/Root", "table-1", 1);

    auto shards1 = GetTableShards(server, sender, "/Root/table-1");

    auto isTableShard = [&](ui64 tabletId) -> bool {
        return std::find(shards1.begin(), shards1.end(), tabletId) != shards1.end();
    };

    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)"));

    SimulateSleep(server, TDuration::Seconds(1));

    // Read the upserted row and also prime shard for unprotected reads
    TString sessionId, txId;
    UNIT_ASSERT_VALUES_EQUAL(
        KqpSimpleBegin(runtime, sessionId, txId, Q_(R"(
            SELECT key, value FROM `/Root/table-1`
            ORDER BY key
            )")),
        "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

    // Make sure we are at the max immediate write edge for current step and it's confirmed
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)"));
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (3, 3)"));

    TString upsertSender = CreateSessionRPC(runtime);
    TString readSender = CreateSessionRPC(runtime);

    // Block commits and start counting propose responses
    TVector<THolder<IEventHandle>> blockedCommits;
    size_t seenProposeResults = 0;
    auto blockCommits = [&](TAutoPtr<IEventHandle>& ev) -> auto {
        switch (ev->GetTypeRewrite()) {
            case TEvTablet::TEvCommit::EventType: {
                auto* msg = ev->Get<TEvTablet::TEvCommit>();
                if (isTableShard(msg->TabletID)) {
                    Cerr << "... blocked commit for tablet " << msg->TabletID << Endl;
                    blockedCommits.push_back(std::move(ev));
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvDataShard::TEvProposeTransactionResult::EventType: {
                Cerr << "... observed propose transaction result" << Endl;
                ++seenProposeResults;
                break;
            }
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    };
    auto prevObserver = runtime.SetObserverFunc(blockCommits);

    auto waitFor = [&](const auto& condition, const TString& description) {
        while (!condition()) {
            Cerr << "... waiting for " << description << Endl;
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return condition();
            };
            runtime.DispatchEvents(options);
        }
    };

    // Start upserting a row with blocked commits, it will stick to the same version as the last upsert
    auto fUpsert = SendRequest(
        runtime,
        MakeSimpleRequestRPC(Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (4, 4)"), upsertSender, "", true));

    waitFor([&]{ return blockedCommits.size() > 0; }, "blocked commit");

    // Start reading data, we know it must read confirmed data, but it will also include the blocked row above
    auto fRead = SendRequest(
        runtime,
        MakeSimpleRequestRPC(Q_("SELECT key, value FROM `/Root/table-1` ORDER BY key"), readSender, "", true));

    // Sleep for 1 second
    SimulateSleep(runtime, TDuration::Seconds(1));

    // We are blocking commits, so read must not see a 4th row until we unblock
    if (seenProposeResults > 0) {
        // We might make it possible in the future to run reads like that without blocking
        // However, it still means we must not return the 4th row that is not committed
        auto response = AwaitResponse(runtime, fRead);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(result),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }");
        return;
    }

    // Unblock all commits
    runtime.SetObserverFunc(prevObserver);
    for (auto& ev : blockedCommits) {
        runtime.Send(ev.Release(), 0, true);
    }
    blockedCommits.clear();

    // We must successfully upsert the row
    {
        auto response = AwaitResponse(runtime, fUpsert);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    // We must successfully read including the 4th row
    {
        auto response = AwaitResponse(runtime, fRead);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Table::ExecuteQueryResult result;
        response.operation().result().UnpackTo(&result);
        UNIT_ASSERT_VALUES_EQUAL(
            FormatResult(result),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
            "{ items { uint32_value: 4 } items { uint32_value: 4 } }");
    }
}

Y_UNIT_TEST(LocksBrokenStats) {
    NKikimrConfig::TAppConfig app;
    TPortManager pm;
    TServerSettings serverSettings(pm.GetPort(2134));
    serverSettings.SetDomainName("Root")
        .SetUseRealThreads(false)
        .SetAppConfig(app);

    Tests::TServer::TPtr server = new TServer(serverSettings);
    auto &runtime = *server->GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

    InitRoot(server, sender);

    TShardedTableOptions opts;
    auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);
    const ui64 shard = shards[0];

    // Insert initial data
    ExecSQL(server, sender, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100);"));

    // Start a KQP transaction with a read (this establishes locks via KQP)
    TString sessionId, txId;
    KqpSimpleBegin(runtime, sessionId, txId, Q_("SELECT * FROM `/Root/table-1` WHERE key = 1;"));
    UNIT_ASSERT(!txId.empty());

    // Set up typed observer to capture TEvWriteResult
    // We need to copy the record data since the event pointer may become invalid
    TMaybe<NKikimrDataEvents::TEvWriteResult> breakerRecord;
    TMaybe<NKikimrDataEvents::TEvWriteResult> victimRecord;
    auto observer = runtime.AddObserver<NEvents::TDataEvents::TEvWriteResult>([&](NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
        auto* result = ev->Get();
        if (result && result->Record.GetOrigin() == shard) {
            if (result->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED) {
                breakerRecord = result->Record;
            } else if (result->Record.GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN) {
                victimRecord = result->Record;
            }
        }
    });

    // Execute SQL using KqpSimpleExec - this will commit immediately and break the locks
    // The observer will capture TEvWriteResult during execution
    KqpSimpleExec(runtime, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 200);"));

    // Verify we captured a COMPLETE result with LocksBrokenAsBreaker set
    UNIT_ASSERT(breakerRecord.Defined());
    UNIT_ASSERT_VALUES_EQUAL(breakerRecord->GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    UNIT_ASSERT(breakerRecord->HasTxStats());
    UNIT_ASSERT_VALUES_EQUAL(breakerRecord->GetTxStats().GetLocksBrokenAsBreaker(), 1u);

    // Now try to commit the victim transaction - it should fail with ABORTED (LOCKS_BROKEN at datashard level)
    auto commitResult = KqpSimpleCommit(runtime, sessionId, txId, Q_("UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 300);"));
    UNIT_ASSERT_VALUES_EQUAL(commitResult, "ERROR: ABORTED");

    // Verify we captured a LOCKS_BROKEN result
    UNIT_ASSERT(victimRecord.Defined());
    UNIT_ASSERT_VALUES_EQUAL(victimRecord->GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);

    auto tableState = ReadTable(server, shards, tableId);
    UNIT_ASSERT(tableState.find("key = 1, value = 200") != TString::npos);
}

} // Y_UNIT_TEST_SUITE(DataShardOutOfOrder)

} // namespace NKikimr
