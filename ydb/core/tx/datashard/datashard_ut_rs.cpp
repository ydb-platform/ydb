#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

ui64 GetRSCount(TTestActorRuntime &runtime, TActorId sender, ui64 shard)
{
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    TString miniKQL =   R"___((
        (let range '('ExcFrom '('TxId (Uint64 '0) (Void))))
        (let select '('TxId))
        (let options '())
        (let pgmReturn (AsList
            (SetResult 'myRes (Length (Member (SelectRange 'InReadSets range select options) 'List)))))
        (return pgmReturn)
    ))___";

    request->Record.MutableProgram()->MutableProgram()->SetText(miniKQL);
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), 0);
    return reply->Record.GetExecutionEngineEvaluatedResponse()
        .GetValue().GetStruct(0).GetOptional().GetUint64();
}

struct IsReadSet {
    IsReadSet(ui64 src, ui64 dest)
        : Source(src)
        , Dest(dest)
    {
    }

    bool operator()(IEventHandle& ev)
    {
        if (ev.GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
            auto &rec = ev.Get<TEvTxProcessing::TEvReadSet>()->Record;
            bool isExpectation = (
                (rec.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET) &&
                (rec.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA));
            if (rec.GetTabletSource() == Source && rec.GetTabletDest() == Dest && !isExpectation) {
                return true;
            }
        }
        return false;
    }

    ui64 Source;
    ui64 Dest;
};

Y_UNIT_TEST_SUITE(TDataShardRSTest) {
    Y_UNIT_TEST(TestCleanupInRS) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // Volatile transactions avoid storing readsets in InReadSets table
            .SetEnableDataShardVolatileTransactions(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        //runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 4, false);

        // Fill some data. Later we will copy data from shards 2 and 3 to shard 1.
        {
            auto request = MakeSQLRequest("UPSERT INTO `/Root/table-1` (key, value) VALUES (0x50000000,1),(0x80000001,1),(0x80000002,1),(0x80000003,1),(0x80000004,1),(0x80000005,1),(0x80000006,1),(0x80000007,1),(0x80000008,1),(0x80000009,1)");
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
            runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(handle);
        }

        auto shards = GetTableShards(server, sender, "/Root/table-1");

        // Add some fake RS to the first shard.
        {
            auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
            TString miniKQL =   R"___((
                (let range (ListFromRange (Uint64 '1) (Uint64 '450001)))
                (let upd (lambda '(x) (UpdateRow 'InReadSets
                                                 '( '('TxId x) '('Origin x) '('From x) '('To x) )
                                                 '('('Body (ByteString '"\x09\x00\x00\x00\x02\x16\x0C\x0B\x00\x00\x00\x00\x00"))))))
                (return (Map range upd))
            ))___";

            request->Record.MutableProgram()->MutableProgram()->SetText(miniKQL);
            runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

            auto reply = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), 0);
        }

        // Run multishard tx but drop RS to pause it on the first shard.
        {
            auto captureRS = [shard=shards[1]](TAutoPtr<IEventHandle> &event) -> auto {
                if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
                    auto &rec = event->Get<TEvTxProcessing::TEvReadSet>()->Record;
                    if (rec.GetTabletSource() == shard)
                        return TTestActorRuntime::EEventAction::DROP;
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            runtime.SetObserverFunc(captureRS);

            auto request = MakeSQLRequest("UPSERT INTO `/Root/table-1` (key, value) SELECT value, key FROM `/Root/table-1` WHERE key = 0x50000000");
            runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
            // Wait until both parts of tx are finished on the second shard.
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsTxResultComplete(), 2);
            runtime.DispatchEvents(options);
        }

        // Run more txs and wait until RSs are in local db.
        {
            for (auto i = 1; i < 10; ++i) {
                auto request = MakeSQLRequest(Sprintf("UPSERT INTO `/Root/table-1` (key, value) SELECT value, key FROM `/Root/table-1` WHERE key = %" PRIu32, i + 0x80000000));
                runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
            }
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTxProcessing::EvReadSetAck, 9);
            runtime.DispatchEvents(options);
        }

        // Now restart the first shard and wait for RS cleanup events.
        {
            UNIT_ASSERT_VALUES_EQUAL(GetRSCount(runtime, sender, shards[0]), 450009);

            runtime.Register(CreateTabletKiller(shards[0]));

            TDispatchOptions options;
            //TDataShard::TEvPrivate::EvRemoveOldInReadSets
            options.FinalEvents.emplace_back(EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 9, 5);
            runtime.DispatchEvents(options);

            // We can't be sure RS are cleaned up because shared event number
            // is used to track cleanup txs. So make a check loop.
            while (GetRSCount(runtime, sender, shards[0]) != 9)
                runtime.DispatchEvents(options, TDuration::Seconds(1));
        }

        // Check all remained RS are for existing txs
        {
            auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
            TString miniKQL =   R"___((
                (let range '('ExcFrom '('TxId (Uint64 '0) (Void))))
                (let select '('TxId))
                (let options '())
                (let pgmReturn (AsList
                    (SetResult 'myRes (SelectRange 'InReadSets range select options))))
                (return pgmReturn)
            ))___";

            request->Record.MutableProgram()->MutableProgram()->SetText(miniKQL);
            runtime.SendToPipe(shards[0], sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto reply = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), 0);
            for (auto &row : reply->Record.GetExecutionEngineEvaluatedResponse()
                     .GetValue().GetStruct(0).GetOptional().GetStruct(0).GetList()) {
                UNIT_ASSERT(row.GetStruct(0).GetOptional().GetUint64() > 450000);
            }
        }

        // Now let all other txs finish and check we have no more RS stored.
        {
            runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
            runtime.Register(CreateTabletKiller(shards[0]));

            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsTxResultComplete(), 10);
            runtime.DispatchEvents(options);

            while (GetRSCount(runtime, sender, shards[0]) != 0)
                runtime.DispatchEvents(options, TDuration::Seconds(1));
        }
    }

    Y_UNIT_TEST(TestDelayedRSAckForUnknownTx) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 4);

        // Fill some data.
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (0x20000000,0x20000001),(0x60000000,0x60000001),(0xA0000000,0xA0000001),(0xE0000000,0xE0000001);");

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto shards2 = GetTableShards(server, sender, "/Root/table-2");

        // Run multishard tx but drop all RS acks to the table-1.
        // Tx should still finish successfully.
        {
            auto captureRS = [shard=shards1[0]](TAutoPtr<IEventHandle> &event) -> auto {
                if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSetAck) {
                    auto &rec = event->Get<TEvTxProcessing::TEvReadSetAck>()->Record;
                    if (rec.GetTabletSource() == shard) {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            runtime.SetObserverFunc(captureRS);

            ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) SELECT key, value FROM `/Root/table-1`");
        }

        // Now we have multishard tx completed but RS not acked. Restart
        // second table shards to trigger RS resend.
        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        for (auto shard : shards2)
            runtime.Register(CreateTabletKiller(shard));

        // Try to drop table which waits for all OutRS to be acked. If acks
        // for completed tx work correctly then it should succeed.
        SendSQL(server, sender, "DROP TABLE `/Root/table-1`", false);
        WaitTabletBecomesOffline(server, shards1[0]);
    }

    Y_UNIT_TEST(TestDelayedRSAckForOutOfOrderCompletedTx) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            // This test expects rs acks to be delayed during one of restarts,
            // which doesn't happen with volatile transactions. With volatile
            // transactions both upserts have already executed, one of them is
            // just waiting for confirmation before making changes visible.
            // Since acks are not delayed they are just gone when dropped.
            .SetEnableDataShardVolatileTransactions(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);
        CreateShardedTable(server, sender, "/Root", "table-3", 1);

        // Fill some data.
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2);");

        ui64 shard1 = GetTableShards(server, sender, "/Root/table-1")[0];
        ui64 shard2 = GetTableShards(server, sender, "/Root/table-2")[0];
        ui64 shard3 = GetTableShards(server, sender, "/Root/table-3")[0];

        // We want to intercept all RS from table-1 and all RS acks
        // from table-3.
        auto captureRS = [shard1,shard3](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSet) {
                auto &rec = event->Get<TEvTxProcessing::TEvReadSet>()->Record;
                bool isExpectation = (
                    (rec.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET) &&
                    (rec.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA));
                if (rec.GetTabletSource() == shard1 && !isExpectation) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            } else if (event->GetTypeRewrite() == TEvTxProcessing::EvReadSetAck) {
                auto &rec = event->Get<TEvTxProcessing::TEvReadSetAck>()->Record;
                if (rec.GetTabletDest() == shard3) {
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureRS);

        // Copy data from table-1 to table-3. Tx should hung due to dropped RS.
        SendSQL(server, sender, "UPSERT INTO `/Root/table-3` (key, value) SELECT key, value FROM `/Root/table-1`");
        // Copy data from table-2 to table-3. Tx should succeed due to out-of-order.
        // RS acks are dropped so table-2 should have unacked RS in local DB.
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-3` (key, value) SELECT key, value FROM `/Root/table-2`");

        // Restart table-3 and wait for new RS from table-2.
        {
            runtime.Register(CreateTabletKiller(shard3));
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsReadSet(shard2, shard3));
            runtime.DispatchEvents(options);
        }
        // Now restart table-2 and wait for repeated RS.
        // This should test duplicated delayed RS acks.
        {
            runtime.Register(CreateTabletKiller(shard2));
            TDispatchOptions options;
            options.FinalEvents.emplace_back(IsReadSet(shard2, shard3));
            runtime.DispatchEvents(options);
        }

        // Now we should have delayed RS in table-3 for table-2 which is waiting
        // tx execution. Set default observer, send table-2 drop request and
        // restart table-1. Restart will cause RS resend and tx will be finished
        // in table-3. Finished tx will make delayed RS outdated and ack will
        // be send to table-2 leading to successful drop.
        runtime.SetObserverFunc(&TTestActorRuntime::DefaultObserverFunc);
        SendSQL(server, sender, "DROP TABLE `/Root/table-2`", false);
        runtime.Register(CreateTabletKiller(shard1));
        WaitTabletBecomesOffline(server, shard2);
    }

    Y_UNIT_TEST(TestGenericReadSetDecisionCommit) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardGenericReadSets(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)");

        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        size_t readSets = 0;
        auto observeReadSets = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    if (msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                        break;
                    }
                    NKikimrTx::TReadSetData genericData;
                    Y_ABORT_UNLESS(genericData.ParseFromString(msg->Record.GetReadSet()));
                    Cerr << "... generic readset: " << genericData.DebugString() << Endl;
                    UNIT_ASSERT(genericData.HasDecision());
                    UNIT_ASSERT(genericData.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT);
                    UNIT_ASSERT(!genericData.HasData());
                    UNIT_ASSERT(genericData.unknown_fields().empty());
                    ++readSets;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserver = runtime.SetObserverFunc(observeReadSets);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (2, 2)
            )"),
            "<empty>");
        UNIT_ASSERT(readSets > 0);
    }

    Y_UNIT_TEST(TestGenericReadSetDecisionAbort) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardGenericReadSets(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        TDisableDataShardLogBatching disableDataShardLogBatching;
        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1)");

        TString sessionId, txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, R"(
                SELECT key, value FROM `/Root/table-1`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 1 } }");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (2, 2)");

        size_t readSets = 0;
        auto observeReadSets = [&](TAutoPtr<IEventHandle> &ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvReadSet::EventType: {
                    auto* msg = ev->Get<TEvTxProcessing::TEvReadSet>();
                    if (msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_NO_DATA) {
                        if (!(msg->Record.GetFlags() & NKikimrTx::TEvReadSet::FLAG_EXPECT_READSET)) {
                            Cerr << "... nodata readset" << Endl;
                            ++readSets;
                        }
                        break;
                    }
                    NKikimrTx::TReadSetData genericData;
                    Y_ABORT_UNLESS(genericData.ParseFromString(msg->Record.GetReadSet()));
                    Cerr << "... generic readset: " << genericData.DebugString() << Endl;
                    UNIT_ASSERT(genericData.HasDecision());
                    UNIT_ASSERT(genericData.GetDecision() == NKikimrTx::TReadSetData::DECISION_ABORT);
                    UNIT_ASSERT(!genericData.HasData());
                    UNIT_ASSERT(genericData.unknown_fields().empty());
                    ++readSets;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserver = runtime.SetObserverFunc(observeReadSets);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, R"(
                UPSERT INTO `/Root/table-2` (key, value) VALUES (3, 3)
            )"),
            "ERROR: ABORTED");
        UNIT_ASSERT(readSets > 0);
    }
}

} // namespace NKikimr
