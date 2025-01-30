#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"
#include "datashard_ut_read_table.h"

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NSchemeShard;
using namespace Tests;
using namespace NDataShardReadTableTest;

Y_UNIT_TEST_SUITE(DataShardReadTableSnapshots) {

    Y_UNIT_TEST(ReadTableSnapshot) {
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
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1), (2, 2), (3, 3);");

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1"));

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");

        auto table1head = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
        UNIT_ASSERT_VALUES_EQUAL(table1head,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");

        Cerr << "---Rebooting tablet---" << Endl;
        RebootTablet(runtime, shards[0], sender);

        // We must be able to finish reading from the acquired snapshot
        auto table1snapshot = table1state.All();
        UNIT_ASSERT_VALUES_EQUAL(table1snapshot,
            "key = 1, value = 1\n"
            "key = 2, value = 2\n"
            "key = 3, value = 3\n");
    }

    Y_UNIT_TEST(ReadTableSplitAfter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        // Read the first row
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Split the first shard on key 3 boundary
        TInstant splitStart = TInstant::Now();
        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards[0], 3);
        Cerr << "--- split started ---" << Endl;
        WaitTxNotification(server, senderSplit, txId);
        Cerr << "--- split finished ---" << Endl;

        TDuration elapsed = TInstant::Now() - splitStart;
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(NValgrind::PlainOrUnderValgrind(2, 10)),
            "Split needed " << elapsed.ToString() << " to complete, which is too long");

        // We should be able to gather the whole table
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");
    }

    Y_UNIT_TEST(ReadTableSplitBefore) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        // Read some rows
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 2, value = 22\n");

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        // Split the first shard on key 2 boundary (that we have seen already)
        TInstant splitStart = TInstant::Now();
        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards[0], 2);
        Cerr << "--- split started ---" << Endl;
        WaitTxNotification(server, senderSplit, txId);
        Cerr << "--- split finished ---" << Endl;

        TDuration elapsed = TInstant::Now() - splitStart;
        UNIT_ASSERT_C(elapsed < TDuration::Seconds(NValgrind::PlainOrUnderValgrind(2, 10)),
            "Split needed " << elapsed.ToString() << " to complete, which is too long");

        // We should be able to gather the whole table
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n");
    }

    Y_UNIT_TEST(ReadTableSplitFinished) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");

        // Split on key=3 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        }

        // Split on key=5 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(1), 5);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 3u);
        }

        // Start reading
        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        // Read some rows until key=3
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 2, value = 22\n");
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 3, value = 33\n");

        // Split on key=2 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 2);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4u);
        }

        // Split on key=6 boundary (so we trigger a new resolve cycle)
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(3), 6);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 5u);
        }

        // We should be able to gather the whole table
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n"
            "key = 5, value = 55\n"
            "key = 6, value = 66\n");
    }

    Y_UNIT_TEST(ReadTableDropColumn) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");

        // Split on key=3 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        }

        // Start reading
        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");

        // Drop the value column
        {
            ui64 txId = AsyncAlterDropColumn(server, "/Root", "table-1", "value");
            WaitTxNotification(server, txId);
        }

        // We should receive an error starting with the second shard
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "ERROR: ResolveError\n");
    }

    Y_UNIT_TEST(ReadTableDropColumnLatePropose) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");

        // Split on key=3 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 3);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        }

        auto shard2actor = ResolveTablet(runtime, shards[1]);

        TVector<THolder<IEventHandle>> capturedPropose;
        auto capturePropose = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvDataShard::TEvProposeTransaction::EventType: {
                    const auto* msg = ev->Get<TEvDataShard::TEvProposeTransaction>();
                    if (msg->Record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCAN && ev->GetRecipientRewrite() == shard2actor) {
                        Cerr << "... captured scan propose at " << shard2actor << Endl;
                        capturedPropose.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(capturePropose);

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

        // Start reading
        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");

        waitFor([&]{ return capturedPropose.size() > 0; }, "propose at second shard");
        runtime.SetObserverFunc(prevObserverFunc);

        // Drop the value column
        {
            ui64 txId = AsyncAlterDropColumn(server, "/Root", "table-1", "value");
            WaitTxNotification(server, txId);
        }

        // Unblock the captured propose
        for (auto& ev : capturedPropose) {
            runtime.Send(ev.Release(), 0, /* viaActorSystem */ true);
        }

        // We should receive an error starting with the second shard
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "ERROR: ResolveError\n");
    }

    Y_UNIT_TEST(ReadTableMaxRows) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");

        // Split on key=4 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 4);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        }

        TVector<size_t> rowLimits;
        auto inspectResponses = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxProcessing::TEvStreamQuotaResponse::EventType: {
                    const auto* msg = ev->Get<TEvTxProcessing::TEvStreamQuotaResponse>();
                    if (msg->Record.HasRowLimit()) {
                        Cerr << "... observed row limit of "
                            << msg->Record.GetRowLimit() << " rows at " << ev->GetRecipientRewrite() << Endl;
                        rowLimits.push_back(msg->Record.GetRowLimit());
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(inspectResponses);

        // Start reading
        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true, 5));

        // We should only receive 4 rows
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n"
            "key = 5, value = 55\n");

        UNIT_ASSERT_VALUES_EQUAL(rowLimits.size(), 6u);
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[0], 5u); // first shard is asked for 5 rows
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[1], 4u);
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[2], 3u);
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[3], 2u); // first shard doesn't know it's out of rows yet
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[4], 2u); // second shard is asked for 2 rows
        UNIT_ASSERT_VALUES_EQUAL(rowLimits[5], 1u);
    }

    Y_UNIT_TEST(ReadTableSplitNewTxIdResolveResultReorder) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44), (5, 55), (6, 66);");

        // Split on key=4 boundary
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(0), 4);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        }

        // Start reading
        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/table-1", true));

        // Read the first row
        UNIT_ASSERT(table1state.Next());
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "key = 1, value = 11\n");

        bool captureTxIds = true;
        TVector<THolder<IEventHandle>> capturedTxIds;
        size_t captureResolveKeySetResultPartitions = 3;
        TVector<THolder<IEventHandle>> capturedResolveKeySetResults;
        auto capturePropose = [&](TAutoPtr<IEventHandle>& ev) -> auto {
            switch (ev->GetTypeRewrite()) {
                case TEvTxUserProxy::TEvAllocateTxIdResult::EventType: {
                    if (captureTxIds) {
                        Cerr << "... captured TEvAllocateTxIdResult" << Endl;
                        capturedTxIds.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvTxProxySchemeCache::TEvResolveKeySetResult::EventType: {
                    const auto* msg = ev->Get<TEvTxProxySchemeCache::TEvResolveKeySetResult>();
                    const auto* request = msg->Request.Get();
                    if (request->ErrorCount > 0) {
                        Cerr << "... ignored TEvResolveKeySetResult with errors" << Endl;
                        break;
                    }
                    size_t partitions = request->ResultSet[0].KeyDescription->GetPartitions().size();
                    if (partitions == captureResolveKeySetResultPartitions) {
                        Cerr << "... captured TEvResolveKeySetResult with " << partitions << " partitions" << Endl;
                        capturedResolveKeySetResults.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    Cerr << "... ignored TEvResolveKeySetResult with " << partitions << " partitions" << Endl;
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(capturePropose);

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

        // Split on key=6 boundary (so we trigger a new resolve cycle)
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards.at(1), 6);
            WaitTxNotification(server, senderSplit, txId);
            shards = GetTableShards(server, sender, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(shards.size(), 3u);
        }

        waitFor([&]{ return capturedTxIds.size() > 0; }, "captured second tx id");
        waitFor([&]{ return capturedResolveKeySetResults.size() > 0; }, "captured new partitions");

        runtime.SetObserverFunc(prevObserverFunc);

        for (auto& ev : capturedResolveKeySetResults) {
            runtime.Send(ev.Release(), 0, true);
        }
        for (auto& ev : capturedTxIds) {
            runtime.Send(ev.Release(), 0, true);
        }

        // We should be able to gather the whole table
        UNIT_ASSERT_VALUES_EQUAL(table1state.All(),
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
            "key = 4, value = 44\n"
            "key = 5, value = 55\n"
            "key = 6, value = 66\n");
    }

    Y_UNIT_TEST(CorruptedDyNumber) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "Table",
            TShardedTableOptions().Columns({
                {"key", "Uint32", true, false},
                {"value", "DyNumber", false, false}
            }));

        // Write bad DyNumber
        UploadRows(runtime, "/Root/Table", 
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::DYNUMBER}},
            {TCell::Make(ui32(1))}, {TCell::Make(ui32(55555))}
            );

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/Table", true));

        UNIT_ASSERT(!table1state.Next());

        UNIT_ASSERT(table1state.IsError);
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "ERROR: ExecError\n");
    }    

} // Y_UNIT_TEST_SUITE(DataShardReadTableSnapshots)

} // namespace NKikimr
