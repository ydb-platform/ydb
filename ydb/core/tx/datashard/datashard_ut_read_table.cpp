#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"
#include "datashard_ut_read_table.h"

#include "datashard_ut_common_kqp.h"
#include <ydb/core/formats/factory.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <util/generic/scope.h>

#include <ydb/core/testlib/actors/block_events.h>

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
        UploadRows(runtime, "/Root", "/Root/Table",
            {{"key", Ydb::Type::UINT32}, {"value", Ydb::Type::DYNUMBER}},
            {TCell::Make(ui32(1))}, {TCell::Make(ui32(55555))}
            );

        auto table1state = TReadTableState(server, MakeReadTableSettings("/Root/Table", true));

        UNIT_ASSERT(!table1state.Next());

        UNIT_ASSERT(table1state.IsError);
        UNIT_ASSERT_VALUES_EQUAL(table1state.LastResult, "ERROR: ExecError\n");
    }

    Y_UNIT_TEST(ReadTableUUID) {
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

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Columns({
                {"key", "Uuid", true, false},
                {"value", "Uuid", false, false}
            }));
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES "
            "(Uuid('5e32442b-4613-40b7-8506-b14d86b74ef1'), Uuid('65d4d0aa-1612-4887-86dd-834e4cfc3df5')), "
            "(Uuid('bbf1d45a-af5f-4939-b58a-2be39fd2f06e'), Uuid('710c9ea5-9fa2-4606-a35f-608d0556eb6f')), "
            "(Uuid('303f21b5-3805-4133-b706-bfc0e071c528'), Uuid('522e048c-9b4b-4d38-8c89-3996cdaa1c8d'));");

        auto table1 = TReadTableState(server, MakeReadTableSettings("/Root/table-1")).All();
        UNIT_ASSERT_VALUES_EQUAL(table1,
            "key = 5e32442b-4613-40b7-8506-b14d86b74ef1, value = 65d4d0aa-1612-4887-86dd-834e4cfc3df5\n"
            "key = bbf1d45a-af5f-4939-b58a-2be39fd2f06e, value = 710c9ea5-9fa2-4606-a35f-608d0556eb6f\n"
            "key = 303f21b5-3805-4133-b706-bfc0e071c528, value = 522e048c-9b4b-4d38-8c89-3996cdaa1c8d\n");
    }

    Y_UNIT_TEST(ReadTableSnapshotAfterMerge) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        InitRoot(server, sender);

        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        auto shards = GetTableShards(server, sender, "/Root/table-1");

        // Pre-split into 2 shards: shard[0] covers [1,2], shard[1] covers [3,4]
        {
            auto senderSplit = runtime.AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards[0], 3);
            WaitTxNotification(server, senderSplit, txId);
        }

        ExecSQL(server, sender,
            "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 11), (2, 22), (3, 33), (4, 44);");

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        // Resolve the tablet actor for shard[0] (range [1,2])
        auto shard0actor = ResolveTablet(runtime, shards[0]);
        Cerr << "Shard 0 id: " << shards[0] << ", actor id: " << shard0actor << Endl;

        // Block TX_KIND_SCAN proposes to shard[0] so its read is held back until we
        // trigger the split.  Shard[1]'s scan proceeds normally.
        NActors::TBlockEvents<TEvDataShard::TEvProposeTransaction> blockedScans(runtime,
            [&](const TEvDataShard::TEvProposeTransaction::TPtr& ev) {
                return ev->Get()->Record.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCAN &&
                       ev->GetRecipientRewrite() == shard0actor;
            });

        // Start ReadTable driver reading only from shard 0 without waiting for TEvReady.
        auto edge = runtime.AllocateEdgeActor();
        auto rts = MakeReadTableSettings("/Root/table-1");
        rts.KeyRange.mutable_less()->mutable_type()->mutable_tuple_type()
            ->add_elements()->mutable_optional_type()->mutable_item()->Settype_id(
                ::Ydb::Type_PrimitiveTypeId::Type_PrimitiveTypeId_UINT32);
        rts.KeyRange.mutable_less()->mutable_value()->add_items()->set_uint32_value(3);
        auto driver = runtime.Register(new TReadTableDriver(edge, rts));
        runtime.EnableScheduleForActor(driver);

        runtime.WaitFor("TX_KIND_SCAN captured", [&] { return blockedScans.size() >= 1; });

        // merge shards 0 and 1 (that doesn't have the snapshot).
        {
            Cerr << "... merging shards" << Endl;
            auto senderMerge = runtime.AllocateEdgeActor();
            ui64 txId = AsyncMergeTable(server, senderMerge, "/Root/table-1", {shards[0], shards[1]});
            WaitTxNotification(server, senderMerge, txId);
            Cerr << "... merge done" << Endl;
        }


        // Release the captured scan to old shard[0]
        // The tx proxy gets WRONG_SHARD_STATE, retries, re-resolves to the merged shard,
        // and re-sends the scan with the original snapshot (step, txId).
        // If there is no snapshot on the merged shard the scan fails with SNAPSHOT_NOT_EXIST.
        blockedScans.Stop();
        blockedScans.Unblock();

        {
            auto ev = runtime.GrabEdgeEventRethrow<TReadTableDriver::TEvReady>(edge);
            Y_UNUSED(ev);
        }
        bool isError = false;
        TStringBuilder result;
        while (true) {
            runtime.Send(
                new IEventHandle(driver, TActorId(), new TReadTableDriver::TEvNext()),
                0, true);
            auto ev = runtime.GrabEdgeEventRethrow<TReadTableDriver::TEvResult>(edge);
            isError = ev->Get()->IsError;
            result << ev->Get()->Result;
            if (ev->Get()->Finished) {
                break;
            }
        }
        // Check that ProxyShardTryLater (that maps to a retryable status) is returned.
        UNIT_ASSERT(isError);
        UNIT_ASSERT_VALUES_EQUAL(result, "ERROR: ProxyShardTryLater\n");
    }

} // Y_UNIT_TEST_SUITE(DataShardReadTableSnapshots)

} // namespace NKikimr

namespace NKikimr {
namespace {

using namespace NDataShard;
using namespace NDataShard::NKqpHelpers;
using namespace NDataShardReadTableTest;

enum class ERead {
    Iterator,
    Columns,
    KqpScan,
    ReadTable,
    MiniKQL,
    AllColumns,
};

class TBuildingColumnFixture {
public:
    TBuildingColumnFixture() {
        Tests::TServerSettings settings(PortManager.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableAddColumsWithDefaults(true);
        settings.Formats = new TFormatFactory;
        Server = new Tests::TServer(settings);
        auto& runtime = *Server->GetRuntime();
        Sender = runtime.AllocateEdgeActor();
        InitRoot(Server, Sender);
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "Table", 1);
        Shard = shards.at(0);
        TableId = tableId;
        ExecSQL(Server, Sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10), (2, 20);");
    }

    NKikimrSchemeOp::TTableDescription Schema() {
        return GetTables(Server, Shard).first.begin()->second.GetDescription();
    }

    ui32 ColumnId(const NKikimrSchemeOp::TTableDescription& schema, const TString& name) {
        for (const auto& column : schema.GetColumns()) {
            if (column.GetName() == name) {
                return column.GetId();
            }
        }
        UNIT_FAIL("Missing column " << name);
        return 0;
    }

    void CheckRead(ERead read, const TString& name, bool building, const TRowVersion& snapshot) {
        auto& runtime = *Server->GetRuntime();
        const auto schema = Schema();
        const auto columnId = ColumnId(schema, name);
        const auto sender = runtime.AllocateEdgeActor();

        switch (read) {
        case ERead::Iterator: {
            auto request = GetBaseReadRequest(TableId, schema, ++ReadId,
                NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            request->Record.ClearColumns();
            request->Record.AddColumns(columnId);
            AddFullRangeQuery(*request);
            runtime.SendToPipe(Shard, sender, request.release());
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(sender);
            const auto& result = *response->Get();
            UNIT_ASSERT_VALUES_EQUAL_C(result.Record.GetStatus().GetCode(),
                building ? Ydb::StatusIds::SCHEME_ERROR : Ydb::StatusIds::SUCCESS,
                result.Record.DebugString());
            if (!building) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetRowsCount(), 3);
                for (size_t i = 0; i < result.GetRowsCount(); ++i) {
                    const auto cells = result.GetCells(i);
                    UNIT_ASSERT(!cells[0].IsNull());
                    UNIT_ASSERT_VALUES_EQUAL(cells[0].AsValue<ui32>(), name == "extra" ? 42 : (i + 1) * 10);
                }
            }
            break;
        }
        case ERead::Columns: {
            auto request = MakeHolder<TEvDataShard::TEvReadColumnsRequest>();
            request->Record.SetTableId(TableId.PathId.LocalPathId);
            request->Record.SetSnapshotStep(snapshot.Step);
            request->Record.SetSnapshotTxId(snapshot.TxId);
            request->Record.AddColumns(name);
            request->Record.SetFromKeyInclusive(true);
            runtime.SendToPipe(Shard, sender, request.Release());
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadColumnsResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(),
                building ? NKikimrTxDataShard::TError::SCHEME_ERROR : 0,
                response->Get()->Record.DebugString());
            if (!building) {
                UNIT_ASSERT(!response->Get()->Record.GetBlocks().empty());
            }
            break;
        }
        case ERead::KqpScan: {
            auto request = MakeHolder<TEvDataShard::TEvKqpScan>();
            auto& record = request->Record;
            record.SetTxId(++ReadId);
            record.SetScanId(ReadId);
            record.SetLocalPathId(TableId.PathId.LocalPathId);
            record.SetTablePath("/Root/Table");
            record.SetSchemaVersion(schema.GetTableSchemaVersion());
            record.AddColumnTags(columnId);
            record.AddColumnTypes(NScheme::NTypeIds::Uint32);
            record.AddColumnTypeInfos();
            auto* range = record.AddRanges();
            range->SetFromInclusive(true);
            range->SetToInclusive(true);
            record.MutableSnapshot()->SetStep(snapshot.Step);
            record.MutableSnapshot()->SetTxId(snapshot.TxId);
            record.SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            runtime.SendToPipe(Shard, sender, request.Release());
            TAutoPtr<IEventHandle> handle;
            const auto [error, init] = runtime.GrabEdgeEventsRethrow<
                NKqp::TEvKqpCompute::TEvScanError, NKqp::TEvKqpCompute::TEvScanInitActor>(handle);
            if (building) {
                UNIT_ASSERT_C(error != nullptr, "KQP scan accepted a building column");
                UNIT_ASSERT_STRING_CONTAINS(error->Record.DebugString(), "column");
            } else {
                UNIT_ASSERT_C(init != nullptr, (error ? error->Record.DebugString() : "Missing scan init"));
                runtime.Send(new IEventHandle(ActorIdFromProto(init->Record.GetScanActorId()), sender,
                    new NKqp::TEvKqpCompute::TEvScanDataAck(1_MB)), 0, true);
                auto response = runtime.GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(sender);
                UNIT_ASSERT_VALUES_EQUAL(response->Get()->Rows.size(), 3);
                for (size_t i = 0; i < response->Get()->Rows.size(); ++i) {
                    const auto& row = response->Get()->Rows[i];
                    UNIT_ASSERT(!row[0].IsNull());
                    UNIT_ASSERT_VALUES_EQUAL(row[0].AsValue<ui32>(), name == "extra" ? 42 : (i + 1) * 10);
                }
            }
            break;
        }
        case ERead::ReadTable: {
            if (building) {
                NKikimrTxDataShard::TDataTransaction tx;
                tx.SetImmediate(true);
                tx.SetReadOnly(true);
                tx.SetStreamResponse(true);
                ActorIdToProto(sender, tx.MutableSink());
                auto& scan = *tx.MutableReadTableTransaction();
                scan.MutableTableId()->SetOwnerId(TableId.PathId.OwnerId);
                scan.MutableTableId()->SetTableId(TableId.PathId.LocalPathId);
                scan.SetSnapshotStep(snapshot.Step);
                scan.SetSnapshotTxId(snapshot.TxId);
                auto& column = *scan.AddColumns();
                column.SetId(columnId);
                column.SetName(name);
                column.SetTypeId(NScheme::NTypeIds::Uint32);
                runtime.SendToPipe(Shard, sender, new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_SCAN, sender, ++ReadId, tx.SerializeAsString(), TTxFlags::Immediate));
                TAutoPtr<IEventHandle> handle;
                const auto [result, clearance] = runtime.GrabEdgeEventsRethrow<
                    TEvDataShard::TEvProposeTransactionResult, TEvTxProcessing::TEvStreamClearanceRequest>(handle);
                Y_UNUSED(clearance);
                UNIT_ASSERT_C(result != nullptr, "ReadTable accepted a building column");
                UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetStatus(),
                    NKikimrTxDataShard::TEvProposeTransactionResult::ERROR, result->Record.DebugString());
                UNIT_ASSERT_STRING_CONTAINS(result->Record.DebugString(), "SCHEME_CHANGED");
            }
            auto settings = MakeReadTableSettings("/Root/Table", true);
            settings.Columns = {name};
            TReadTableState state(Server, settings);
            const auto result = state.All();
            UNIT_ASSERT_VALUES_EQUAL_C(state.IsError, building, result);
            if (!building) {
                UNIT_ASSERT_VALUES_EQUAL(result, name == "extra"
                    ? "extra = 42\nextra = 42\nextra = 42\n"
                    : "value = 10\nvalue = 20\nvalue = 30\n");
            }
            break;
        }
        case ERead::MiniKQL: {
            const auto body = CompileSelectRow(schema, name);
            runtime.SendToPipe(Shard, sender, new TEvDataShard::TEvProposeTransaction(
                NKikimrTxDataShard::TX_KIND_DATA, sender, ++ReadId, body, snapshot, TTxFlags::Immediate));
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), building
                ? NKikimrTxDataShard::TEvProposeTransactionResult::ERROR
                : NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE,
                response->Get()->Record.DebugString());
            break;
        }
        case ERead::AllColumns: {
            auto settings = MakeReadTableSettings("/Root/Table", true);
            TReadTableState state(Server, settings);
            const auto result = state.All();
            UNIT_ASSERT_C(!state.IsError, result);
            UNIT_ASSERT_STRING_CONTAINS(result, "key = 1");
            UNIT_ASSERT_STRING_CONTAINS(result, "value = 30");
            if (building) {
                UNIT_ASSERT_C(!result.Contains("extra"), result);
            } else {
                UNIT_ASSERT_STRING_CONTAINS(result, "extra = 42");
            }
            break;
        }
        }
    }

    TString CompileSelectRow(const NKikimrSchemeOp::TTableDescription& schema, const TString& name) {
        using namespace NMiniKQL;
        using TResolver = NYql::IDbSchemeResolver;
        TMockDbSchemeResolver resolver;
        TResolver::TTableResult table(TResolver::TTableResult::Ok);
        table.Table.TableName = "Table";
        table.TableId.Reset(new TTableId(TableId.PathId, schema.GetTableSchemaVersion()));
        table.KeyColumnCount = 1;
        for (const auto& column : schema.GetColumns()) {
            table.Columns.emplace(column.GetName(), TResolver::TTableResult::TColumn{
                column.GetId(), column.GetName() == "key" ? 0 : -1,
                NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), 0, EColumnTypeConstraint::Nullable});
        }
        resolver.AddTable(table);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        alloc.Release();
        Y_DEFER { alloc.Acquire(); };
        auto* registry = Server->GetRuntime()->GetAppData().FunctionRegistry;
        auto compiled = NYql::ConvertToMiniKQL(NYql::ParseText(TStringBuilder()
            << "((let row '('('key (Uint32 '1)))) (let columns '('" << name << ")) "
            << "(return (AsList (SetResult 'result (SelectRow 'Table row columns)))))"),
            registry, &env, &resolver).GetValueSync();
        UNIT_ASSERT_C(compiled.Node.GetNode(), compiled.Errors.ToString());
        auto engine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1,
            registry, *TAppData::RandomProvider, *TAppData::TimeProvider));
        UNIT_ASSERT(engine->SetProgram(SerializeRuntimeNode(compiled.Node, env)) == IEngineFlat::EResult::Ok);
        for (auto& key : engine->GetDbKeys()) {
            key->Status = TKeyDesc::EStatus::Ok;
            key->Partitioning = std::make_shared<TPartitioning>(TVector<TKeyDesc::TPartitionInfo>{Shard});
        }
        UNIT_ASSERT(engine->PrepareShardPrograms() == IEngineFlat::EResult::Ok);
        IEngineFlat::TShardData shard;
        UNIT_ASSERT(engine->GetAffectedShard(0, shard) == IEngineFlat::EResult::Ok);
        NKikimrTxDataShard::TDataTransaction tx;
        tx.SetMiniKQL(shard.Program);
        tx.SetImmediate(true);
        tx.SetReadOnly(true);
        return tx.SerializeAsString();
    }

    TPortManager PortManager;
    Tests::TServer::TPtr Server;
    TActorId Sender;
    TTableId TableId;
    ui64 Shard = 0;
    ui64 ReadId = 100000;
};

void TestBuildingColumn(ERead read, bool reboot = false) {
    TBuildingColumnFixture fixture;
    auto& runtime = *fixture.Server->GetRuntime();
    auto batches = runtime.AddObserver<NSchemeShard::TEvIndexBuilder::TEvCreateRequest>([](auto& event) {
        event->Get()->Record.MutableSettings()->MutableScanSettings()->SetMaxBatchRows(1);
        event->Get()->Record.MutableSettings()->MutableScanSettings()->SetMaxBatchBytes(1);
    });
    TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(runtime);
    auto build = KqpSchemeExecSend(runtime,
        "ALTER TABLE `/Root/Table` ADD COLUMN extra Uint32 DEFAULT 42;", "/Root");
    runtime.WaitFor("first backfill batch", [&] { return !uploads.empty() || build.HasValue(); });
    UNIT_ASSERT_C(!uploads.empty(), build.GetValueSync().DebugString());
    uploads.Unblock(1);
    runtime.WaitFor("second backfill batch", [&] { return !uploads.empty(); });
    ExecSQL(fixture.Server, fixture.Sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (3, 30);");
    if (reboot) {
        RebootTablet(runtime, fixture.Shard, fixture.Sender);
    }
    auto snapshot = CreateVolatileSnapshot(fixture.Server, {"/Root/Table"});
    fixture.CheckRead(read == ERead::AllColumns ? ERead::Iterator : read, "value", false, snapshot);
    fixture.CheckRead(read, "extra", true, snapshot);
    uploads.Stop().Unblock();
    UNIT_ASSERT_VALUES_EQUAL(KqpSchemeExecWait(runtime, build), "SUCCESS");
    snapshot = CreateVolatileSnapshot(fixture.Server, {"/Root/Table"});
    fixture.CheckRead(read, "extra", false, snapshot);
    fixture.CheckRead(ERead::Iterator, "extra", false, snapshot);
    RebootTablet(runtime, fixture.Shard, fixture.Sender);
    fixture.CheckRead(read, "extra", false, snapshot);
}

} // namespace

Y_UNIT_TEST_SUITE(DataShardBuildingColumns) {
    Y_UNIT_TEST(PreserveBuildFlagInSchemaDelta) {
        NKikimrSchemeOp::TTableDescription schema;
        schema.SetName("Table");
        schema.SetPath("/Root/Table");
        auto* key = schema.AddColumns();
        key->SetId(1);
        key->SetName("key");
        key->SetTypeId(NScheme::NTypeIds::Uint32);
        schema.AddKeyColumnIds(1);
        auto* extra = schema.AddColumns();
        extra->SetId(2);
        extra->SetName("extra");
        extra->SetTypeId(NScheme::NTypeIds::Uint32);
        extra->SetIsBuildInProgress(true);
        TUserTable table(1000, schema, 0);

        NKikimrSchemeOp::TTableDescription delta;
        delta.SetName("Table");
        auto* column = delta.AddColumns();
        column->SetId(2);
        column->SetName("extra");
        TUserTable altered(table, delta);
        altered.GetSchema(schema);
        UNIT_ASSERT(schema.GetColumns(1).GetIsBuildInProgress());

        TUserTable restored(1000, schema, 0);
        restored.SetPath("/Root/Table");
        restored.GetSchema(schema);
        UNIT_ASSERT(schema.GetColumns(1).GetIsBuildInProgress());

        column->SetIsBuildInProgress(false);
        TUserTable ready(restored, delta);
        ready.GetSchema(schema);
        UNIT_ASSERT(!schema.GetColumns(1).GetIsBuildInProgress());
    }

    Y_UNIT_TEST(ReadIterator) { TestBuildingColumn(ERead::Iterator); }
    Y_UNIT_TEST(ReadIteratorAfterReboot) { TestBuildingColumn(ERead::Iterator, true); }
    Y_UNIT_TEST(ReadColumns) { TestBuildingColumn(ERead::Columns); }
    Y_UNIT_TEST(KqpScan) { TestBuildingColumn(ERead::KqpScan); }
    Y_UNIT_TEST(ReadTable) { TestBuildingColumn(ERead::ReadTable); }
    Y_UNIT_TEST(SelectRow) { TestBuildingColumn(ERead::MiniKQL); }
    Y_UNIT_TEST(ReadTableAllColumns) { TestBuildingColumn(ERead::AllColumns); }
}

} // namespace NKikimr
