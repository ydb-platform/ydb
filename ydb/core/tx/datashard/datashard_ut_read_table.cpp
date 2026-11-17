#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_active_transaction.h"
#include "datashard_ut_read_table.h"

#include "datashard_ut_common_kqp.h"
#include <ydb/core/formats/factory.h>
#include <ydb/core/tx/schemeshard/index/build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <util/generic/scope.h>
#include <util/system/unaligned_mem.h>

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

using TProjection = TVector<TString>;
using TRow = TVector<ui32>;
using TRows = TVector<TRow>;

enum class ERead { Iterator, Columns, KqpScan, ReadTable, MiniKQL };
enum class EColumnState { Building, Ready, Absent };

// The fixture only uses Nullable(UInt32) columns in ClickHouse native blocks.
class TNativeBlockReader {
public:
    explicit TNativeBlockReader(TStringBuf data) : Data(data) {}

    template<class T>
    T Pod() {
        UNIT_ASSERT(Data.size() >= sizeof(T));
        const auto value = ReadUnaligned<T>(Data.data());
        Data.Skip(sizeof(T));
        return value;
    }

    ui64 VarUInt() {
        ui64 result = 0;
        for (ui32 shift = 0; shift < 64; shift += 7) {
            const auto byte = Pod<ui8>();
            result |= ui64(byte & 0x7f) << shift;
            if (!(byte & 0x80)) {
                return result;
            }
        }
        UNIT_FAIL("Invalid native block VarUInt");
        return 0;
    }

    TString String() {
        const auto size = VarUInt();
        UNIT_ASSERT(Data.size() >= size);
        TString result(Data.SubStr(0, size));
        Data.Skip(size);
        return result;
    }

    TRows Read(const TProjection& projection) {
        TRows result;
        while (!Data.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(VarUInt(), 1); // is_overflows
            UNIT_ASSERT_VALUES_EQUAL(Pod<ui8>(), 0);
            UNIT_ASSERT_VALUES_EQUAL(VarUInt(), 2); // bucket_num
            UNIT_ASSERT_VALUES_EQUAL(Pod<i32>(), -1);
            UNIT_ASSERT_VALUES_EQUAL(VarUInt(), 0);
            UNIT_ASSERT_VALUES_EQUAL(VarUInt(), projection.size());
            const auto count = VarUInt();
            const auto offset = result.size();
            result.resize(offset + count, TRow(projection.size()));
            for (size_t column = 0; column < projection.size(); ++column) {
                UNIT_ASSERT_VALUES_EQUAL(String(), projection[column]);
                UNIT_ASSERT_VALUES_EQUAL(String(), "Nullable(UInt32)");
                for (size_t row = 0; row < count; ++row) {
                    UNIT_ASSERT_VALUES_EQUAL(Pod<ui8>(), 0); // NULL bitmap
                }
                for (size_t row = 0; row < count; ++row) {
                    result[offset + row][column] = Pod<ui32>();
                }
            }
        }
        return result;
    }

private:
    TStringBuf Data;
};

class TBuildingColumnFixture {
public:
    TBuildingColumnFixture() {
        Tests::TServerSettings settings(PortManager.GetPort(2134));
        settings.SetDomainName("Root").SetUseRealThreads(false);
        settings.FeatureFlags.SetEnableAddColumsWithDefaults(true);
        settings.Formats = new TFormatFactory;
        Server = new Tests::TServer(settings);
        Sender = Runtime().AllocateEdgeActor();
        InitRoot(Server, Sender);
        auto [shards, tableId] = CreateShardedTable(Server, Sender, "/Root", "Table", 1);
        TableId = tableId;
        ExecSQL(Server, Sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (1, 10), (2, 20);");
        RememberColumns(Schema(shards.at(0)));
    }

    TTestActorRuntime& Runtime() { return *Server->GetRuntime(); }

    TVector<ui64> Shards() { return GetTableShards(Server, Sender, "/Root/Table"); }

    NKikimrSchemeOp::TTableDescription Schema(ui64 shard) {
        return GetTables(Server, shard).first.begin()->second.GetDescription();
    }

    void RememberColumns(const NKikimrSchemeOp::TTableDescription& schema) {
        for (const auto& column : schema.GetColumns()) {
            ColumnIds[column.GetName()] = column.GetId();
        }
    }

    ui32 Value(ui32 key, const TString& name) const {
        if (name == "key") { return key; }
        if (name == "value") { return key * 10; }
        if (name == "extra") { return 42; }
        UNIT_ASSERT_VALUES_EQUAL(name, "other");
        return 73;
    }

    TRows Expected(const TProjection& projection, const TVector<ui32>& keys) const {
        TRows rows;
        for (const auto key : keys) {
            auto& row = rows.emplace_back();
            for (const auto& column : projection) {
                row.push_back(Value(key, column));
            }
        }
        return rows;
    }

    void AssertRows(TRows actual, const TProjection& projection, const TVector<ui32>& keys) const {
        auto expected = Expected(projection, keys);
        // Protocols may return different row orders; preserve column order and multiplicity.
        Sort(actual);
        Sort(expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
        for (size_t row = 0; row < expected.size(); ++row) {
            UNIT_ASSERT_VALUES_EQUAL(actual[row].size(), projection.size());
            for (size_t column = 0; column < projection.size(); ++column) {
                UNIT_ASSERT_VALUES_EQUAL_C(actual[row][column], expected[row][column],
                    "row " << row << ", column " << projection[column]);
            }
        }
    }

    TVector<ui32> KeysInShard(const NKikimrSchemeOp::TTableDescription& schema) const {
        TSerializedCellVec begin(schema.GetPartitionRangeBegin());
        TSerializedCellVec end(schema.GetPartitionRangeEnd());
        TVector<ui32> result;
        for (auto key : Keys) {
            if (!begin.GetCells().empty() && !begin.GetCells()[0].IsNull()) {
                const auto bound = begin.GetCells()[0].AsValue<ui32>();
                if (key < bound || (key == bound && !schema.GetPartitionRangeBeginIsInclusive())) { continue; }
            }
            if (!end.GetCells().empty() && !end.GetCells()[0].IsNull()) {
                const auto bound = end.GetCells()[0].AsValue<ui32>();
                if (key > bound || (key == bound && !schema.GetPartitionRangeEndIsInclusive())) { continue; }
            }
            result.push_back(key);
        }
        return result;
    }

    TString RejectedColumn(const TProjection& projection, EColumnState state) const {
        if (state != EColumnState::Ready) {
            for (const auto& name : projection) {
                if (name == "extra" || name == "other") { return name; }
            }
        }
        return {};
    }

    static void AppendCells(TRows& rows, TConstArrayRef<TCell> cells, size_t count) {
        UNIT_ASSERT_VALUES_EQUAL(cells.size(), count);
        auto& row = rows.emplace_back();
        for (const auto& cell : cells) {
            UNIT_ASSERT(!cell.IsNull());
            UNIT_ASSERT_VALUES_EQUAL(cell.Size(), sizeof(ui32));
            row.push_back(cell.AsValue<ui32>());
        }
    }

    TRows DecodeResultSet(const TString& data, const TProjection& projection, bool orderedColumns = true) {
        Ydb::ResultSet proto;
        UNIT_ASSERT(proto.ParseFromString(data));
        NYdb::TResultSet result(proto);
        TProjection names;
        for (const auto& column : result.GetColumnsMeta()) { names.emplace_back(column.Name); }
        if (orderedColumns) { UNIT_ASSERT(names == projection); }
        auto expected = projection;
        Sort(names);
        Sort(expected);
        UNIT_ASSERT(names == expected);
        TRows rows;
        NYdb::TResultSetParser parser(result);
        while (parser.TryNextRow()) {
            auto& row = rows.emplace_back();
            for (const auto& name : projection) {
                auto& value = parser.ColumnParser(name);
                const bool optional = value.GetKind() == NYdb::TTypeParser::ETypeKind::Optional;
                if (optional) { value.OpenOptional(); UNIT_ASSERT(!value.IsNull()); }
                UNIT_ASSERT(value.GetKind() == NYdb::TTypeParser::ETypeKind::Primitive);
                UNIT_ASSERT(value.GetPrimitiveType() == NYdb::EPrimitiveType::Uint32);
                row.push_back(value.GetUint32());
                if (optional) { value.CloseOptional(); }
            }
        }
        return rows;
    }

    TRows ReadShard(ERead read, ui64 shard, const TProjection& projection,
        EColumnState state, const TRowVersion& snapshot)
    {
        auto& runtime = Runtime();
        const auto schema = Schema(shard);
        const auto badColumn = RejectedColumn(projection, state);
        const auto sender = runtime.AllocateEdgeActor();
        TRows rows;
        switch (read) {
        case ERead::Iterator: {
            auto request = GetBaseReadRequest(TableId, schema, ++ReadId,
                NKikimrDataEvents::FORMAT_CELLVEC, snapshot);
            request->Record.ClearColumns();
            for (const auto& name : projection) { request->Record.AddColumns(ColumnIds.at(name)); }
            AddFullRangeQuery(*request);
            runtime.SendToPipe(shard, sender, request.release());
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadResult>(sender);
            const auto& result = *response->Get();
            UNIT_ASSERT_VALUES_EQUAL_C(result.Record.GetStatus().GetCode(), badColumn
                ? Ydb::StatusIds::SCHEME_ERROR : Ydb::StatusIds::SUCCESS, result.Record.DebugString());
            if (badColumn) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetRowsCount(), 0);
                UNIT_ASSERT_VALUES_EQUAL(result.Record.GetStatus().IssuesSize(), 1);
                const auto& issue = result.Record.GetStatus().GetIssues(0);
                UNIT_ASSERT_VALUES_EQUAL(issue.severity(), ui32(NYql::TSeverityIds::S_ERROR));
                UNIT_ASSERT_VALUES_EQUAL(issue.message(), TStringBuilder()
                    << "Unknown column: " << ColumnIds.at(badColumn) << " (shard# " << shard
                    << " node# " << ResolveTablet(runtime, shard).NodeId()
                    << " state# " << DatashardStateName(TShardState::Ready) << ")");
            } else {
                UNIT_ASSERT_VALUES_EQUAL(result.Record.GetStatus().IssuesSize(), 0);
                UNIT_ASSERT(result.Record.GetFinished());
                for (size_t i = 0; i < result.GetRowsCount(); ++i) {
                    AppendCells(rows, result.GetCells(i), projection.size());
                }
            }
            break;
        }
        case ERead::Columns: {
            auto request = MakeHolder<TEvDataShard::TEvReadColumnsRequest>();
            request->Record.SetTableId(TableId.PathId.LocalPathId);
            request->Record.SetSnapshotStep(snapshot.Step);
            request->Record.SetSnapshotTxId(snapshot.TxId);
            request->Record.SetFromKeyInclusive(true);
            request->Record.SetMaxRows(1); // Decode multiple native blocks too.
            for (const auto& name : projection) { request->Record.AddColumns(name); }
            runtime.SendToPipe(shard, sender, request.Release());
            auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReadColumnsResponse>(sender);
            const auto& result = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), badColumn
                ? NKikimrTxDataShard::TError::SCHEME_ERROR : 0, result.DebugString());
            if (badColumn) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetErrorDescription(), "Unknown column: " + badColumn);
                UNIT_ASSERT(result.GetBlocks().empty());
            } else {
                UNIT_ASSERT(result.GetErrorDescription().empty());
                UNIT_ASSERT(result.GetEndOfShard());
                rows = TNativeBlockReader(result.GetBlocks()).Read(projection);
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
            for (const auto& name : projection) {
                record.AddColumnTags(ColumnIds.at(name));
                record.AddColumnTypes(NScheme::NTypeIds::Uint32);
                record.AddColumnTypeInfos();
            }
            auto* range = record.AddRanges();
            range->SetFromInclusive(true);
            range->SetToInclusive(true);
            record.MutableSnapshot()->SetStep(snapshot.Step);
            record.MutableSnapshot()->SetTxId(snapshot.TxId);
            record.SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
            runtime.SendToPipe(shard, sender, request.Release());
            TAutoPtr<IEventHandle> handle;
            const auto [error, init] = runtime.GrabEdgeEventsRethrow<
                NKqp::TEvKqpCompute::TEvScanError, NKqp::TEvKqpCompute::TEvScanInitActor>(handle);
            if (badColumn) {
                UNIT_ASSERT_C(error != nullptr, "KQP scan accepted a hidden column");
                UNIT_ASSERT(!init);
                UNIT_ASSERT_VALUES_EQUAL(error->Record.GetStatus(), Ydb::StatusIds::ABORTED);
                UNIT_ASSERT_VALUES_EQUAL(error->Record.GetTabletId(), shard);
                UNIT_ASSERT_VALUES_EQUAL(error->Record.IssuesSize(), 1);
                const auto& issue = error->Record.GetIssues(0);
                UNIT_ASSERT_VALUES_EQUAL(issue.issue_code(), ui32(NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH));
                UNIT_ASSERT_VALUES_EQUAL(issue.message(), TStringBuilder() << "TxId: " << ReadId << "."
                    << " Cant find table '/Root/Table' column " << ColumnIds.at(badColumn) << " at " << shard);
            } else {
                UNIT_ASSERT_C(init != nullptr, (error ? error->Record.DebugString() : "Missing scan init"));
                const auto actor = ActorIdFromProto(init->Record.GetScanActorId());
                for (;;) {
                    runtime.Send(new IEventHandle(actor, sender, new NKqp::TEvKqpCompute::TEvScanDataAck(1_MB)), 0, true);
                    auto response = runtime.GrabEdgeEventRethrow<NKqp::TEvKqpCompute::TEvScanData>(sender);
                    for (const auto& row : response->Get()->Rows) { AppendCells(rows, row, projection.size()); }
                    if (response->Get()->Finished) { break; }
                }
            }
            break;
        }
        case ERead::ReadTable: {
            // Exercise the raw protocol with the same registered snapshot as the
            // other readers, including successful projections and streaming acks.
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
            scan.SetApiVersion(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1);
            scan.MutableRange()->SetFromInclusive(true);
            scan.MutableRange()->SetToInclusive(true);
            for (const auto& name : projection) {
                auto& column = *scan.AddColumns();
                column.SetId(ColumnIds.at(name));
                column.SetName(name);
                column.SetTypeId(NScheme::NTypeIds::Uint32);
            }
            runtime.SendToPipe(shard, sender, new TEvDataShard::TEvProposeTransaction(
                NKikimrTxDataShard::TX_KIND_SCAN, sender, ++ReadId, tx.SerializeAsString(), TTxFlags::Immediate));
            TAutoPtr<IEventHandle> handle;
            const auto [result, clearance] = runtime.GrabEdgeEventsRethrow<
                TEvDataShard::TEvProposeTransactionResult, TEvTxProcessing::TEvStreamClearanceRequest>(handle);
            if (badColumn) {
                UNIT_ASSERT_C(result != nullptr, "ReadTable accepted a hidden column");
                UNIT_ASSERT(!clearance);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::ERROR);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.ErrorSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError(0).GetKind(), NKikimrTxDataShard::TError::SCHEME_CHANGED);
                UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError(0).GetReason(), TStringBuilder()
                    << "ReadTable cannot find column " << badColumn << " (" << ColumnIds.at(badColumn) << ")");
                UNIT_ASSERT(result->Record.GetTxResult().empty());
                break;
            }
            UNIT_ASSERT(clearance != nullptr);
            UNIT_ASSERT_VALUES_EQUAL(clearance->Record.GetTxId(), ReadId);
            UNIT_ASSERT_VALUES_EQUAL(clearance->Record.GetShardId(), shard);
            auto cleared = MakeHolder<TEvTxProcessing::TEvStreamClearanceResponse>();
            cleared->Record.SetTxId(ReadId);
            cleared->Record.SetCleared(true);
            runtime.Send(new IEventHandle(handle->Sender, sender, cleared.Release(), 0, handle->Cookie), 0, true);
            ui64 sequence = 1;
            for (;;) {
                const auto [data, quota, release] = runtime.GrabEdgeEventsRethrow<
                    TEvDataShard::TEvProposeTransactionResult, TEvTxProcessing::TEvStreamQuotaRequest,
                    TEvTxProcessing::TEvStreamQuotaRelease>(handle);
                if (release) { UNIT_ASSERT_VALUES_EQUAL(release->Record.GetTxId(), ReadId); continue; }
                if (quota) {
                    UNIT_ASSERT_VALUES_EQUAL(quota->Record.GetTxId(), ReadId);
                    auto response = MakeHolder<TEvTxProcessing::TEvStreamQuotaResponse>();
                    response->Record.SetTxId(ReadId);
                    response->Record.SetMessageSizeLimit(1_MB);
                    response->Record.SetMessageRowsLimit(1);
                    response->Record.SetReservedMessages(1);
                    runtime.Send(new IEventHandle(handle->Sender, sender, response.Release(), 0, handle->Cookie), 0, true);
                    continue;
                }
                UNIT_ASSERT(data != nullptr);
                const auto& record = data->Record;
                UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), ReadId);
                UNIT_ASSERT_VALUES_EQUAL(record.GetOrigin(), shard);
                UNIT_ASSERT_VALUES_EQUAL(record.ErrorSize(), 0);
                if (record.GetStatus() == NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE) {
                    UNIT_ASSERT(record.GetTxResult().empty());
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrTxDataShard::TEvProposeTransactionResult::RESPONSE_DATA);
                UNIT_ASSERT_VALUES_EQUAL(record.GetApiVersion(), ui32(NKikimrTxUserProxy::TReadTableTransaction::YDB_V1));
                UNIT_ASSERT_VALUES_EQUAL(record.GetDataSeqNo(), sequence++);
                auto part = DecodeResultSet(record.GetTxResult(), projection);
                UNIT_ASSERT_VALUES_EQUAL(part.size(), 1);
                rows.insert(rows.end(), part.begin(), part.end());
                auto ack = MakeHolder<TEvTxProcessing::TEvStreamDataAck>();
                runtime.Send(new IEventHandle(handle->Sender, sender, ack.Release()), 0, true);
            }
            break;
        }
        case ERead::MiniKQL: {
            for (const auto key : KeysInShard(schema)) {
                auto [body, engine] = CompileSelectRow(schema, projection, shard, key);
                runtime.SendToPipe(shard, sender, new TEvDataShard::TEvProposeTransaction(
                    NKikimrTxDataShard::TX_KIND_DATA, sender, ++ReadId, body, snapshot, TTxFlags::Immediate));
                auto response = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvProposeTransactionResult>(sender);
                const auto& result = response->Get()->Record;
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), badColumn
                    ? NKikimrTxDataShard::TEvProposeTransactionResult::ERROR
                    : NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE, result.DebugString());
                if (badColumn) {
                    UNIT_ASSERT_VALUES_EQUAL(result.ErrorSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(result.GetError(0).GetKind(), NKikimrTxDataShard::TError::SCHEME_ERROR);
                    UNIT_ASSERT(result.GetTxResult().empty());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(result.ErrorSize(), 0);
                    engine->AddShardReply(shard, result.GetTxResult());
                    engine->FinalizeOriginReplies(shard);
                    engine->BuildResult();
                    UNIT_ASSERT(engine->GetStatus() == NMiniKQL::IEngineFlat::EStatus::Complete);
                    NKikimrMiniKQL::TResult value;
                    UNIT_ASSERT(engine->FillResultValue(value) == NMiniKQL::IEngineFlat::EResult::Ok);
                    UNIT_ASSERT_VALUES_EQUAL(value.GetType().GetStruct().MemberSize(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(value.GetType().GetStruct().GetMember(0).GetName(), "result");
                    const auto& resultType = value.GetType().GetStruct().GetMember(0).GetType();
                    UNIT_ASSERT(resultType.GetKind() == NKikimrMiniKQL::Optional);
                    const auto& selectedType = resultType.GetOptional().GetItem();
                    UNIT_ASSERT(selectedType.GetKind() == NKikimrMiniKQL::Optional);
                    UNIT_ASSERT(selectedType.GetOptional().GetItem().GetKind() == NKikimrMiniKQL::Struct);
                    const auto& rowType = selectedType.GetOptional().GetItem().GetStruct();
                    UNIT_ASSERT_VALUES_EQUAL(value.GetValue().StructSize(), 1);
                    UNIT_ASSERT(value.GetValue().GetStruct(0).HasOptional());
                    const auto& selected = value.GetValue().GetStruct(0).GetOptional();
                    UNIT_ASSERT(selected.HasOptional());
                    const auto& rowValue = selected.GetOptional();
                    UNIT_ASSERT_VALUES_EQUAL(rowType.MemberSize(), projection.size());
                    UNIT_ASSERT_VALUES_EQUAL(rowValue.StructSize(), projection.size());
                    TRow row;
                    for (const auto& name : projection) {
                        bool found = false;
                        for (size_t i = 0; i < rowType.MemberSize(); ++i) {
                            if (rowType.GetMember(i).GetName() == name) {
                                const auto& field = rowValue.GetStruct(i);
                                UNIT_ASSERT(field.HasOptional());
                                UNIT_ASSERT(field.GetOptional().HasUint32());
                                row.push_back(field.GetOptional().GetUint32());
                                found = true;
                                break;
                            }
                        }
                        UNIT_ASSERT_C(found, name);
                    }
                    rows.push_back(std::move(row));
                }
            }
            break;
        }
        }
        return rows;
    }

    void CheckReadTable(const TProjection& projection, EColumnState state) {
        const auto badColumn = RejectedColumn(projection, state);
        TProjection expectedProjection = projection;
        if (projection.empty()) {
            expectedProjection = {"key", "value"};
            if (state == EColumnState::Ready) { expectedProjection.insert(expectedProjection.end(), {"extra", "other"}); }
        }
        TRows rows;
        NKikimrTxUserProxy::TEvProposeTransactionStatus error;
        auto observer = Runtime().AddObserver<TEvTxUserProxy::TEvProposeTransactionStatus>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResponseData) {
                auto part = DecodeResultSet(record.GetSerializedReadTableResponse(), expectedProjection, !projection.empty());
                rows.insert(rows.end(), part.begin(), part.end());
            } else if (record.GetStatus() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
                error = record;
            }
        });
        auto settings = MakeReadTableSettings("/Root/Table", true);
        settings.Columns = projection;
        TReadTableState reader(Server, settings);
        reader.All();
        UNIT_ASSERT_VALUES_EQUAL_C(reader.IsError, bool(badColumn), error.DebugString());
        if (badColumn) {
            UNIT_ASSERT(rows.empty());
            UNIT_ASSERT_VALUES_EQUAL(error.GetStatus(), TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError);
            UNIT_ASSERT_VALUES_EQUAL(ui32(error.GetStatusCode()), ui32(NKikimrIssues::TStatusIds::SCHEME_ERROR));
            TProjection missing;
            for (const auto& name : projection) {
                if (name == "extra" || name == "other") { missing.push_back(name); }
            }
            Sort(missing);
            UNIT_ASSERT_VALUES_EQUAL(error.IssuesSize(), missing.size());
            for (size_t i = 0; i < error.IssuesSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(error.GetIssues(i).issue_code(), ui32(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR));
                UNIT_ASSERT_VALUES_EQUAL(error.GetIssues(i).message(), "Unresolved column: '" + missing[i] + "'");
            }
        } else {
            UNIT_ASSERT(!error.HasStatus());
            AssertRows(rows, expectedProjection, Keys);
        }
    }

    void CheckProjection(ERead read, const TProjection& projection, EColumnState state, const TRowVersion& snapshot) {
        TRows rows;
        for (const auto shard : Shards()) {
            auto part = ReadShard(read, shard, projection, state, snapshot);
            rows.insert(rows.end(), part.begin(), part.end());
        }
        if (read == ERead::ReadTable) {
            CheckReadTable(projection, state);
        }
        if (RejectedColumn(projection, state)) {
            UNIT_ASSERT(rows.empty());
        } else {
            AssertRows(rows, projection, Keys);
        }
    }

    // Reused without weakening expectations across every build/lifecycle transition.
    void CheckInvariants(const TVector<ERead>& reads, EColumnState state) {
        for (auto shard : Shards()) {
            const auto schema = Schema(shard);
            RememberColumns(schema);
            THashSet<TString> names;
            for (const auto& column : schema.GetColumns()) {
                names.insert(column.GetName());
                UNIT_ASSERT_VALUES_EQUAL(column.GetIsBuildInProgress(),
                    state == EColumnState::Building && (column.GetName() == "extra" || column.GetName() == "other"));
            }
            THashSet<TString> expected = {"key", "value"};
            if (state != EColumnState::Absent) { expected.insert("extra"); expected.insert("other"); }
            UNIT_ASSERT(names == expected);
        }
        const auto snapshot = CreateVolatileSnapshot(Server, {"/Root/Table"});
        for (auto read : reads) {
            CheckProjection(read, {"value", "key"}, state, snapshot);
            for (const auto& projection : TVector<TProjection>{
                    {"extra"}, {"other"}, {"value", "extra", "key"}, {"other", "key", "extra", "value"}}) {
                CheckProjection(read, projection, state, snapshot);
            }
        }
        CheckReadTable({}, state);
    }

    void CheckInvariants(ERead read, EColumnState state) {
        CheckInvariants(TVector<ERead>{read}, state);
    }

    void CheckInvariants(EColumnState state) {
        CheckInvariants({ERead::Iterator, ERead::Columns, ERead::KqpScan, ERead::ReadTable, ERead::MiniKQL}, state);
    }

    std::pair<TString, TAutoPtr<NMiniKQL::IEngineFlat>> CompileSelectRow(
        const NKikimrSchemeOp::TTableDescription& schema, const TProjection& projection, ui64 shardId, ui32 key)
    {
        using namespace NMiniKQL;
        using TResolver = NYql::IDbSchemeResolver;
        TMockDbSchemeResolver resolver;
        TResolver::TTableResult table(TResolver::TTableResult::Ok);
        table.Table.TableName = "Table";
        table.TableId.Reset(new TTableId(TableId.PathId, schema.GetTableSchemaVersion()));
        table.KeyColumnCount = 1;
        for (const auto& [name, id] : ColumnIds) {
            table.Columns.emplace(name, TResolver::TTableResult::TColumn{
                id, name == "key" ? 0 : -1, NScheme::TTypeInfo(NScheme::NTypeIds::Uint32), 0, EColumnTypeConstraint::Nullable});
        }
        resolver.AddTable(table);
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        alloc.Release();
        Y_DEFER { alloc.Acquire(); };
        TStringBuilder columns;
        for (const auto& name : projection) { columns << "'" << name << " "; }
        auto* registry = Runtime().GetAppData().FunctionRegistry;
        auto compiled = NYql::ConvertToMiniKQL(NYql::ParseText(TStringBuilder()
            << "((let row '('('key (Uint32 '" << key << ")))) (let columns '(" << columns << ")) "
            << "(return (AsList (SetResult 'result (SelectRow 'Table row columns)))))"),
            registry, &env, &resolver).GetValueSync();
        UNIT_ASSERT_C(compiled.Node.GetNode(), compiled.Errors.ToString());
        auto engine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1,
            registry, *TAppData::RandomProvider, *TAppData::TimeProvider));
        UNIT_ASSERT(engine->SetProgram(SerializeRuntimeNode(compiled.Node, env)) == IEngineFlat::EResult::Ok);
        for (auto& key : engine->GetDbKeys()) {
            key->Status = TKeyDesc::EStatus::Ok;
            key->Partitioning = std::make_shared<TPartitioning>(TVector<TKeyDesc::TPartitionInfo>{shardId});
        }
        UNIT_ASSERT(engine->PrepareShardPrograms() == IEngineFlat::EResult::Ok);
        IEngineFlat::TShardData shard;
        UNIT_ASSERT(engine->GetAffectedShard(0, shard) == IEngineFlat::EResult::Ok);
        engine->AfterShardProgramsExtracted();
        NKikimrTxDataShard::TDataTransaction tx;
        tx.SetMiniKQL(shard.Program);
        tx.SetImmediate(true);
        tx.SetReadOnly(true);
        return {tx.SerializeAsString(), std::move(engine)};
    }

    void InsertDuringBuild() {
        ExecSQL(Server, Sender, "UPSERT INTO `/Root/Table` (key, value) VALUES (3, 30);");
        Keys.push_back(3);
    }

    void RestartDataShards() {
        for (auto shard : Shards()) { RebootTablet(Runtime(), shard, Sender); }
    }

    void RestartSchemeShard() { RebootTablet(Runtime(), TableId.PathId.OwnerId, Sender); }

    void CheckSplitLocked(ui64 shard, ui32 key, ui64 lockOwner) {
        SetSplitMergePartCountLimit(&Runtime(), -1);
        const auto before = Shards();
        const auto sender = Runtime().AllocateEdgeActor();
        const auto txId = ++ReadId;
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, TableId.PathId.OwnerId);
        auto& modify = *request->Record.AddTransaction();
        modify.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);
        auto& desc = *modify.MutableSplitMergeTablePartitions();
        desc.SetTablePath("/Root/Table");
        desc.AddSourceTabletId(shard);
        desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint32(key);
        Runtime().SendToPipe(TableId.PathId.OwnerId, sender, request.Release());
        const auto response = Runtime().GrabEdgeEventRethrow<TEvSchemeShard::TEvModifySchemeTransactionResult>(sender);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NKikimrScheme::StatusMultipleModifications, record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(record.GetTxId(), txId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeshardId(), TableId.PathId.OwnerId);
        UNIT_ASSERT_VALUES_EQUAL(record.GetReason(), TStringBuilder()
            << "path '" << TPathId(TableId.PathId.OwnerId, TableId.PathId.LocalPathId)
            << "' has been locked by tx: " << lockOwner);
        UNIT_ASSERT(Shards() == before);
    }

    void Split(ui64 shard, ui32 key) {
        SetSplitMergePartCountLimit(&Runtime(), -1);
        const auto before = Shards();
        WaitTxNotification(Server, Sender, AsyncSplitTable(Server, Sender, "/Root/Table", shard, key));
        const auto after = Shards();
        UNIT_ASSERT_VALUES_EQUAL(after.size(), before.size() + 1);
        UNIT_ASSERT(Find(after, shard) == after.end());
    }

    void PrimeShardDescriptionCache() {
        auto* ss = dynamic_cast<NSchemeShard::TSchemeShard*>(Runtime().FindActor(ResolveTablet(Runtime(), TableId.PathId.OwnerId)));
        UNIT_ASSERT(ss);
        const TPathId pathId(TableId.PathId.OwnerId, TableId.PathId.LocalPathId);
        auto table = ss->Tables.at(pathId);
        table->FillDescriptionCache(ss->PathsById.at(pathId));
        UNIT_ASSERT(table->TableDescription.HasPathId());
        THashSet<TString> building;
        for (const auto& column : table->TableDescription.GetColumns()) {
            if (column.GetIsBuildInProgress()) { building.insert(column.GetName()); }
        }
        UNIT_ASSERT((building == THashSet<TString>{"extra", "other"}));
    }

    void CheckSchemeShardColumns(EColumnState state) {
        const auto sender = Runtime().AllocateEdgeActor();
        Runtime().SendToPipe(TableId.PathId.OwnerId, sender, new TEvSchemeShard::TEvDescribeScheme("/Root/Table"));
        const auto response = Runtime().GrabEdgeEventRethrow<TEvSchemeShard::TEvDescribeSchemeResult>(sender);
        const auto description = response->Get()->GetRecord();
        UNIT_ASSERT_VALUES_EQUAL(description.GetStatus(), NKikimrScheme::StatusSuccess);
        THashSet<TString> names;
        for (const auto& column : description.GetPathDescription().GetTable().GetColumns()) { names.insert(column.GetName()); }
        THashSet<TString> expected = {"key", "value"};
        if (state != EColumnState::Absent) { expected.insert("extra"); expected.insert("other"); }
        UNIT_ASSERT(names == expected);
    }

    TPortManager PortManager;
    Tests::TServer::TPtr Server;
    TActorId Sender;
    TTableId TableId;
    ui64 ReadId = 100000;
    TVector<ui32> Keys = {1, 2};
    THashMap<TString, ui32> ColumnIds;
};

class TColumnBuild {
public:
    explicit TColumnBuild(TBuildingColumnFixture& fixture)
        : Fixture(fixture)
        , Uploads(fixture.Runtime())
        , Finalization(fixture.Runtime(), [](const auto& event) {
            if (event->Get()->Record.GetTxKind() != NKikimrTxDataShard::TX_KIND_SCHEME) { return false; }
            NKikimrTxDataShard::TFlatSchemeTransaction tx;
            UNIT_ASSERT(tx.ParseFromString(event->Get()->Record.GetTxBody()));
            return tx.HasFinalizeBuildIndex();
        })
        , Completion(fixture.Runtime().AddObserver<TEvSchemeShard::TEvNotifyTxCompletionResult>([this](auto& event) {
            if (event->Get()->Record.GetTxId() == BuildId) { Complete = true; }
        }))
        , Lock(fixture.Runtime().AddObserver<TEvSchemeShard::TEvModifySchemeTransaction>([this](auto& event) {
            for (const auto& tx : event->Get()->Record.GetTransaction()) {
                if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateLock
                        && tx.GetWorkingDir() == "/Root" && tx.GetLockConfig().GetName() == "Table") {
                    LockOwner = tx.GetLockConfig().GetLockTxId();
                }
            }
        }))
        , Responses(fixture.Runtime().AddObserver<TEvDataShard::TEvUploadRowsResponse>([this](auto& event) {
            if (CheckingBatches) {
                UNIT_ASSERT_VALUES_EQUAL_C(event->Get()->Record.GetStatus(), ui32(NKikimrTxDataShard::TError::OK), event->Get()->Record.DebugString());
                ++Acknowledged;
            }
        }))
    {
        BuildId = ++fixture.ReadId;
        NKikimrIndexBuilder::TIndexBuildSettings settings;
        settings.set_source_path("/Root/Table");
        settings.set_max_shards_in_flight(2);
        settings.MutableScanSettings()->SetMaxBatchRows(1);
        settings.MutableScanSettings()->SetMaxBatchBytes(1);
        for (const auto& name : TProjection{"extra", "other"}) {
            auto* column = settings.mutable_column_build_operation()->add_column();
            column->SetColumnName(name);
            auto* literal = column->mutable_default_from_literal();
            literal->mutable_type()->set_type_id(Ydb::Type::UINT32);
            literal->mutable_value()->set_uint32_value(fixture.Value(0, name));
        }
        const auto sender = fixture.Runtime().AllocateEdgeActor();
        fixture.Runtime().SendToPipe(fixture.TableId.PathId.OwnerId, sender,
            new NSchemeShard::TEvIndexBuilder::TEvCreateRequest(BuildId, "/Root", std::move(settings)));
        const auto response = fixture.Runtime().GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvCreateResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.IssuesSize(), 0);
        WatchCompletion();
        WaitForUpload();
        UNIT_ASSERT(LockOwner);
        fixture.RememberColumns(fixture.Schema(fixture.Shards().at(0)));
    }

    void WatchCompletion() {
        const auto sender = Fixture.Runtime().AllocateEdgeActor();
        Fixture.Runtime().SendToPipe(Fixture.TableId.PathId.OwnerId, sender,
            new TEvSchemeShard::TEvNotifyTxCompletion(BuildId));
    }

    void WaitForUpload() {
        Fixture.Runtime().WaitFor("backfill batch", [&] { return !Uploads.empty() || Complete; });
        UNIT_ASSERT_C(!Uploads.empty(), GetState().DebugString());
    }

    void CommitBatch(ui32 key) {
        WaitForUpload();
        const auto& request = Uploads.front()->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(request.RowsSize(), 1);
        TSerializedCellVec cells(request.GetRows(0).GetKeyColumns());
        UNIT_ASSERT_VALUES_EQUAL(cells.GetCells().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cells.GetCells()[0].AsValue<ui32>(), key);
        UNIT_ASSERT_VALUES_EQUAL(request.GetRowScheme().ValueColumnIdsSize(), 2);
        TSerializedCellVec values(request.GetRows(0).GetValueColumns());
        UNIT_ASSERT_VALUES_EQUAL(values.GetCells().size(), 2);
        for (size_t i = 0; i < values.GetCells().size(); ++i) {
            const auto id = request.GetRowScheme().GetValueColumnIds(i);
            const TString name = id == Fixture.ColumnIds.at("extra") ? "extra" : "other";
            UNIT_ASSERT_VALUES_EQUAL(id, Fixture.ColumnIds.at(name));
            UNIT_ASSERT(!values.GetCells()[i].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(values.GetCells()[i].AsValue<ui32>(), Fixture.Value(key, name));
        }
        const auto before = Acknowledged;
        Uploads.Unblock(1);
        Fixture.Runtime().WaitFor("committed backfill batch", [&] { return Acknowledged > before; });
        UNIT_ASSERT_VALUES_EQUAL(Acknowledged, before + 1);
    }

    void WaitForFinalization() {
        WatchCompletion(); // SchemeShard restart loses in-memory subscriptions.
        CheckingBatches = false;
        Uploads.Stop().Unblock();
        Fixture.Runtime().WaitFor("blocked finalization", [&] { return !Finalization.empty() || Complete; });
        UNIT_ASSERT_C(!Finalization.empty(), GetState().DebugString());
    }

    void Finish() {
        Finalization.Stop().Unblock();
        Fixture.Runtime().WaitFor("completed column build", [&] { return Complete; });
        UNIT_ASSERT_VALUES_EQUAL(GetState().GetState(), Ydb::Table::IndexBuildState::STATE_DONE);
    }

    void Cancel() {
        CheckingBatches = false;
        auto& runtime = Fixture.Runtime();
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(Fixture.TableId.PathId.OwnerId, sender,
            new NSchemeShard::TEvIndexBuilder::TEvCancelRequest(++Fixture.ReadId, "/Root", BuildId));
        const auto response = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvCancelResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        Finalization.Stop().Unblock();
        runtime.WaitFor("cancelled column build", [&] { return Complete; });
        UNIT_ASSERT_VALUES_EQUAL(GetState().GetState(), Ydb::Table::IndexBuildState::STATE_CANCELLED);
        Uploads.Stop();
        Uploads.clear(); // Requests owned by the cancelled scanner must not be replayed.
    }

    NKikimrIndexBuilder::TIndexBuild GetState() {
        auto& runtime = Fixture.Runtime();
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(Fixture.TableId.PathId.OwnerId, sender,
            new NSchemeShard::TEvIndexBuilder::TEvGetRequest("/Root", BuildId));
        const auto response = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvIndexBuilder::TEvGetResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS, response->Get()->Record.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.IssuesSize(), 0);
        return response->Get()->Record.GetIndexBuild();
    }

    TBuildingColumnFixture& Fixture;
    TBlockEvents<TEvDataShard::TEvUploadRowsRequest> Uploads;
    TBlockEvents<TEvDataShard::TEvProposeTransaction> Finalization;
    TTestActorRuntime::TEventObserverHolder Completion;
    TTestActorRuntime::TEventObserverHolder Lock;
    TTestActorRuntime::TEventObserverHolder Responses;
    ui64 BuildId = 0;
    ui64 LockOwner = 0;
    size_t Acknowledged = 0;
    bool CheckingBatches = true;
    bool Complete = false;
};

void TestBuildingColumn(ERead read, bool restart = false) {
    TBuildingColumnFixture fixture;
    fixture.CheckReadTable({}, EColumnState::Absent);
    TColumnBuild build(fixture);
    fixture.CheckInvariants(read, EColumnState::Building); // no uploads yet
    build.CommitBatch(1);
    fixture.CheckInvariants(read, EColumnState::Building);
    fixture.InsertDuringBuild();
    fixture.CheckInvariants(read, EColumnState::Building);
    if (restart) {
        fixture.RestartDataShards();
        fixture.CheckInvariants(read, EColumnState::Building);
    } else {
        build.CommitBatch(2);
        fixture.CheckInvariants(read, EColumnState::Building);
    }
    build.WaitForFinalization();
    fixture.CheckInvariants(read, EColumnState::Building); // all uploads committed, schema still building
    build.Finish();
    fixture.CheckInvariants(read, EColumnState::Ready);
    fixture.RestartDataShards();
    fixture.CheckInvariants(read, EColumnState::Ready);
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
    Y_UNIT_TEST(SchemeShardRestartDuringBackfill) {
        TBuildingColumnFixture fixture;
        TColumnBuild build(fixture);
        build.CommitBatch(1);
        fixture.InsertDuringBuild();
        fixture.CheckInvariants(EColumnState::Building);
        fixture.RestartSchemeShard();
        fixture.CheckInvariants(EColumnState::Building);
        build.WaitForFinalization();
        fixture.CheckInvariants(EColumnState::Building);
        build.Finish();
        fixture.CheckInvariants(EColumnState::Ready);
        fixture.RestartSchemeShard();
        fixture.RestartDataShards();
        fixture.CheckInvariants(EColumnState::Ready);
    }

    Y_UNIT_TEST(ShardDescriptionCacheAfterFinalization) {
        TBuildingColumnFixture fixture;
        TColumnBuild build(fixture);
        build.CommitBatch(1);
        fixture.InsertDuringBuild();
        fixture.CheckInvariants(EColumnState::Building);
        // Populate the actual cache used for creating split destinations, while
        // both column flags are still true. A path description uses another cache.
        fixture.PrimeShardDescriptionCache();
        build.WaitForFinalization();
        fixture.CheckInvariants(EColumnState::Building);
        build.Finish();
        fixture.CheckInvariants(EColumnState::Ready);
        fixture.Split(fixture.Shards().at(0), 2);
        fixture.CheckInvariants(EColumnState::Ready);
        fixture.RestartDataShards();
        fixture.CheckInvariants(EColumnState::Ready);
    }

    Y_UNIT_TEST(SplitRejectedDuringBackfillThenAllowedAfterFinalization) {
        TBuildingColumnFixture fixture;
        TColumnBuild build(fixture);
        build.CommitBatch(1);
        fixture.InsertDuringBuild();
        fixture.CheckInvariants(EColumnState::Building);
        fixture.CheckSplitLocked(fixture.Shards().at(0), 2, build.LockOwner);
        fixture.CheckInvariants(EColumnState::Building);
        build.WaitForFinalization();
        fixture.CheckInvariants(EColumnState::Building);
        build.Finish();
        fixture.CheckInvariants(EColumnState::Ready);
        fixture.Split(fixture.Shards().at(0), 2);
        fixture.CheckInvariants(EColumnState::Ready);
        fixture.RestartSchemeShard();
        fixture.RestartDataShards();
        fixture.CheckInvariants(EColumnState::Ready);
    }

    Y_UNIT_TEST(CancelAfterPartialBackfill) {
        TBuildingColumnFixture fixture;
        TColumnBuild build(fixture);
        build.CommitBatch(1);
        fixture.InsertDuringBuild();
        fixture.CheckInvariants(EColumnState::Building);
        build.Cancel();
        fixture.CheckSchemeShardColumns(EColumnState::Absent);
        fixture.CheckInvariants(EColumnState::Absent);
        fixture.RestartSchemeShard();
        fixture.RestartDataShards();
        fixture.CheckSchemeShardColumns(EColumnState::Absent);
        fixture.CheckInvariants(EColumnState::Absent);
    }
}

} // namespace NKikimr
