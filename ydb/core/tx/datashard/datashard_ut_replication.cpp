#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "const.h"
#include "datashard_active_transaction.h"
#include "datashard_ut_common_kqp.h"

#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/string/strip.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

Y_UNIT_TEST_SUITE(DataShardReplication) {

    Y_UNIT_TEST(SimpleApplyChanges) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Global)
        );
        CreateShardedTable(server, sender, "/Root", "table-2", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Global)
        );

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto shards2 = GetTableShards(server, sender, "/Root/table-2");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");
        auto tableId2 = ResolveTableId(server, sender, "/Root/table-2");

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 234, .Key = 2, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 345, .Key = 2, .Value = 33 },
        });

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 1, .WriteTxId = 234, .Key = 2, .Value = 22 },
        });

        ApplyChanges(server, shards2.at(0), tableId2, "my-source", {
            TChange{ .Offset = 3, .WriteTxId = 345, .Key = 4, .Value = 44 },
        });

        CommitWrites(server, { "/Root/table-1" }, 123);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n");
        }

        CommitWrites(server, { "/Root/table-1" }, 234);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n"
                "key = 2, value = 22\n");
        }

        CommitWrites(server, { "/Root/table-1", "/Root/table-2" }, 345);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 11\n"
                "key = 2, value = 33\n");
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-2");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 4, value = 44\n");
        }
    }

    Y_UNIT_TEST_TWIN(SplitReplicationSourceOffsets, RebootSrc) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        NKikimrConfig::TAppConfig app;
        app.MutableFeatureFlags()->SetEnableTabletRestartOnUnhandledExceptions(true);
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetAppConfig(app);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
        );

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const ui64 srcShard = shards.at(0);

        ApplyChanges(server, srcShard, tableId, "source-a", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 0, .Key = 5, .Value = 55 },
        });
        ApplyChanges(server, srcShard, tableId, "source-b", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 0, .Key = 5, .Value = 55 },
        });

        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", srcShard, 5);
        WaitTxNotification(server, senderSplit, txId);

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);
        const ui64 leftShard = shards.at(0);
        const ui64 rightShard = shards.at(1);

        ApplyChanges(server, leftShard, tableId, "source-a", {
            TChange{ .Offset = 10, .WriteTxId = 0, .Key = 1, .Value = 111 },
        });
        ApplyChanges(server, leftShard, tableId, "source-b", {
            TChange{ .Offset = 10, .WriteTxId = 0, .Key = 1, .Value = 111 },
        });
        ApplyChanges(server, rightShard, tableId, "source-a", {
            TChange{ .Offset = 20, .WriteTxId = 0, .Key = 5, .Value = 555 },
        });
        ApplyChanges(server, rightShard, tableId, "source-b", {
            TChange{ .Offset = 20, .WriteTxId = 0, .Key = 5, .Value = 555 },
        });

        for (ui64 shardId : shards) {
            CompactTable(runtime, shardId, tableId, true);
        }

        txId = AsyncMergeTable(server, senderSplit, "/Root/table-1", shards);
        WaitTxNotification(server, senderSplit, txId);

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u);
        const ui64 mergedShard = shards.at(0);

        // Force src to chunk after each split key.
        auto forceSmallWindow = runtime.AddObserver<TEvDataShard::TEvGetReplicationSourceOffsets>(
            [&](TEvDataShard::TEvGetReplicationSourceOffsets::TPtr& ev) {
                ev->Get()->Record.SetWindowSize(1);
            });

        TTestActorRuntime::TEventObserverHolder rebootObserver;
        bool rebooted = false;
        if (RebootSrc) {
            // Reboot src during offset fetch.
            rebootObserver = runtime.AddObserver<TEvDataShard::TEvGetReplicationSourceOffsets>(
                [&](TEvDataShard::TEvGetReplicationSourceOffsets::TPtr& /*ev*/) {
                    if (!rebooted) {
                        rebooted = true;
                        RebootTablet(runtime, mergedShard, sender);
                    }
                });
        }

        AsyncSplitTable(server, senderSplit, "/Root/table-1", mergedShard, 5);

        // Split stays on 1 shard if dst crashes in a restart loop.
        for (int i = 0; i < 30 && GetTableShards(server, sender, "/Root/table-1").size() < 2; ++i) {
            TDispatchOptions opts;
            runtime.DispatchEvents(opts, TDuration::Seconds(1));
        }

        shards = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 2u);

        auto result = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(result,
            "key = 1, value = 111\n"
            "key = 5, value = 555\n");
    }

    Y_UNIT_TEST_TWIN(SplitMergeChanges, WithReboots) {
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

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Global)
        );
        CreateShardedTable(server, sender, "/Root", "table-2", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Global)
        );

        auto shards1 = GetTableShards(server, sender, "/Root/table-1");
        auto tableId1 = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 123, .Key = 1, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 123, .Key = 5, .Value = 33 },
            TChange{ .Offset = 3, .WriteTxId = 123, .Key = 5, .Value = 44 },
        });

        // Split would fail otherwise :(
        SetSplitMergePartCountLimit(server->GetRuntime(), -1);

        auto senderSplit = runtime.AllocateEdgeActor();
        ui64 txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(0), 5);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 2u);

        txId = AsyncSplitTable(server, senderSplit, "/Root/table-1", shards1.at(1), 10);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 3u);

        // Compact tables so we can merge them later
        for (ui64 shardId : shards1) {
            CompactTable(runtime, shardId, tableId1, true);
        }

        if (WithReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // We expect this change to be ignored (let's pretend this was from some very old worker)
        ApplyChanges(server, shards1.at(1), tableId1, "my-source", {
            TChange{ .Offset = 2, .WriteTxId = 123, .Key = 5, .Value = 33 },
        });

        // Apply some newer changes that are specific to the right shard
        ApplyChanges(server, shards1.at(1), tableId1, "my-source", {
            TChange{ .Offset = 8, .WriteTxId = 123, .Key = 6, .Value = 77 },
            TChange{ .Offset = 9, .WriteTxId = 123, .Key = 6, .Value = 88 },
        });

        CommitWrites(server, { "/Root/table-1" }, 123);

        if (WithReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 22\n"
                "key = 5, value = 44\n"
                "key = 6, value = 88\n");
        }

        if (WithReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // Merge shards back into a single shard
        txId = AsyncMergeTable(server, senderSplit, "/Root/table-1", shards1);
        WaitTxNotification(server, senderSplit, txId);

        shards1 = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1u);

        if (WithReboots) {
            for (ui64 shardId : shards1) {
                RebootTablet(runtime, shardId, sender);
            }
        }

        // We expect changes 4-7 to be applied, but change 8 to be ignored, then 10 applied
        ApplyChanges(server, shards1.at(0), tableId1, "my-source", {
            TChange{ .Offset = 4, .WriteTxId = 234, .Key = 2, .Value = 91 },
            TChange{ .Offset = 5, .WriteTxId = 234, .Key = 2, .Value = 92 },
            TChange{ .Offset = 6, .WriteTxId = 234, .Key = 10, .Value = 93 },
            TChange{ .Offset = 7, .WriteTxId = 234, .Key = 10, .Value = 94 },
            TChange{ .Offset = 8, .WriteTxId = 234, .Key = 6, .Value = 77 },
            TChange{ .Offset = 10, .WriteTxId = 234, .Key = 7, .Value = 95 },
        });

        CommitWrites(server, { "/Root/table-1" }, 234);

        {
            auto result = ReadShardedTable(server, "/Root/table-1");
            UNIT_ASSERT_VALUES_EQUAL(result,
                "key = 1, value = 22\n"
                "key = 2, value = 92\n"
                "key = 5, value = 44\n"
                "key = 6, value = 88\n"
                "key = 7, value = 95\n"
                "key = 10, value = 94\n");
        }
    }

    Y_UNIT_TEST(ReplicatedTable) {
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

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions().Replicated(true));

        ExecSQL(server, sender, "SELECT * FROM `/Root/table-1`");
        ExecSQL(server, sender, "INSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);", true,
            Ydb::StatusIds::BAD_REQUEST);

        WaitTxNotification(server, sender, AsyncAlterDropReplicationConfig(server, "/Root", "table-1"));
        ExecSQL(server, sender, "INSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);");
    }

    Y_UNIT_TEST(ApplyChangesToReplicatedTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
        );

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
            TChange{ .Offset = 1, .WriteTxId = 0, .Key = 2, .Value = 22 },
            TChange{ .Offset = 2, .WriteTxId = 0, .Key = 3, .Value = 33 },
        });

        auto result = ReadShardedTable(server, "/Root/table-1");
        UNIT_ASSERT_VALUES_EQUAL(result,
            "key = 1, value = 11\n"
            "key = 2, value = 22\n"
            "key = 3, value = 33\n"
        );
    }

    Y_UNIT_TEST(ApplyChangesToCommonTable) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions());

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
        }, NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED);
    }

    Y_UNIT_TEST(ApplyChangesWithConcurrentTx) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
        );

        auto shards = GetTableShards(server, sender, "/Root/table-1");
        auto tableId = ResolveTableId(server, sender, "/Root/table-1");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11 },
        });

        TString sessionId;
        TString txId;
        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleBegin(runtime, sessionId, txId, "SELECT key, value FROM `/Root/table-1`;"),
            "{ items { uint32_value: 1 } items { uint32_value: 11 } }");

        ApplyChanges(server, shards.at(0), tableId, "my-source", {
            TChange{ .Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 21 },
        });

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleCommit(runtime, sessionId, txId, "SELECT key, value FROM `/Root/table-1`;"),
            "{ items { uint32_value: 1 } items { uint32_value: 11 } }");
    }

    void WaitForContent(TServer::TPtr server, const TString& tablePath, const TString& expected) {
        for (ui32 attempt = 0; attempt < 30; ++attempt) {
            auto content = ReadShardedTable(server, tablePath);
            if (StripInPlace(content) == expected) {
                return;
            }
            SimulateSleep(server, TDuration::Seconds(1));
        }

        UNIT_ASSERT_VALUES_EQUAL(StripString(ReadShardedTable(server, tablePath)), expected);
    }

    Y_UNIT_TEST(AsyncIndexFromRowConsistentBase) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();
        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
            .Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}})
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_value/indexImplTable";

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11},
        });
        WaitForContent(server, indexPath, "value = 11, key = 1");

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 22},
        });
        WaitForContent(server, indexPath, "value = 22, key = 1");

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 11},
        });
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, indexPath), "value = 22, key = 1\n");

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 2, .WriteTxId = 0, .Key = 1, .Value = 0, .Operation = TChange::EOperation::Erase},
        });
        WaitForContent(server, indexPath, "");

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 3, .WriteTxId = 0, .Key = 1, .Value = 33, .Operation = TChange::EOperation::Reset},
        });
        WaitForContent(server, indexPath, "value = 33, key = 1");
    }

    Y_UNIT_TEST(AsyncIndexRejectsOversizedKeyAtomically) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "String", false, false},
            })
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
            .Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}})
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_value/indexImplTable";

        using TEvResult = NKikimrTxDataShard::TEvApplyReplicationChangesResult;
        auto apply = [&](const TVector<TString>& values, TEvResult::EStatus expectedStatus, TEvResult::EReason expectedReason) {
            auto request = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>(tableId.PathId, tableId.SchemaVersion);
            request->Record.SetSource("source");
            for (ui32 i = 0; i < values.size(); ++i) {
                auto* change = request->Record.AddChanges();
                change->SetSourceOffset(i);
                const TCell keyCell = TCell::Make(i + 1);
                change->SetKey(TSerializedCellVec::Serialize({&keyCell, 1}));
                auto* upsert = change->MutableUpsert();
                upsert->AddTags(2);
                const TCell valueCell(values[i].data(), values[i].size());
                upsert->SetData(TSerializedCellVec::Serialize({&valueCell, 1}));
            }

            auto replyTo = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, replyTo, request.Release(), 0, GetPipeConfigWithRetries());
            auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(replyTo);
            const auto status = result->Get()->Record.GetStatus();
            UNIT_ASSERT_C(status == expectedStatus,
                "Unexpected status " << TEvResult::EStatus_Name(status)
                << ", expected " << TEvResult::EStatus_Name(expectedStatus));
            const auto reason = result->Get()->Record.GetReason();
            UNIT_ASSERT_C(reason == expectedReason,
                "Unexpected reason " << TEvResult::EReason_Name(reason)
                << ", expected " << TEvResult::EReason_Name(expectedReason));
        };

        apply({"ok", TString(NLimits::MaxWriteKeySize + 1, 'x')}, TEvResult::STATUS_REJECTED, TEvResult::REASON_BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "");
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, indexPath), "");

        RebootTablet(runtime, shard, sender);
        apply({"ok"}, TEvResult::STATUS_OK, TEvResult::REASON_NONE);
        UNIT_ASSERT_C(!ReadShardedTable(server, "/Root/table-1").empty(), "The rejected batch advanced the source offset");
    }

    Y_UNIT_TEST(AsyncIndexFromGloballyConsistentBase) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();
        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Global)
            .Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}})
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_value/indexImplTable";

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 0, .WriteTxId = 123, .Key = 1, .Value = 11},
        });
        WaitForContent(server, indexPath, "value = 11, key = 1");
        RebootTablet(*server->GetRuntime(), shard, sender);

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 1, .WriteTxId = 234, .Key = 1, .Value = 22},
        });
        WaitForContent(server, indexPath, "value = 22, key = 1");
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "");

        CommitWrites(server, {"/Root/table-1"}, 123);
        CommitWrites(server, {"/Root/table-1"}, 234);
        WaitForContent(server, indexPath, "value = 22, key = 1");
    }

    Y_UNIT_TEST(AsyncIndexNewSourceAfterPageFault) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto sender = server->GetRuntime()->AllocateEdgeActor();
        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
            .ExecutorCacheSize(1)
            .Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}})
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_value/indexImplTable";

        ApplyChanges(server, shard, tableId, "first", {
            TChange{.Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11},
        });
        WaitForContent(server, indexPath, "value = 11, key = 1");

        CompactTable(*server->GetRuntime(), shard, tableId, false);
        RebootTablet(*server->GetRuntime(), shard, sender);
        ApplyChanges(server, shard, tableId, "second", {
            TChange{.Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 22},
        });
        WaitForContent(server, indexPath, "value = 22, key = 1");

        RebootTablet(*server->GetRuntime(), shard, sender);
        ApplyChanges(server, shard, tableId, "second", {
            TChange{.Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 33},
        });
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 22\n");
        WaitForContent(server, indexPath, "value = 22, key = 1");
    }

    Y_UNIT_TEST(AsyncIndexRejectsChangeQueueOverflow) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetChangesQueueItemsLimit(1);

        Tests::TServer::TPtr server = new TServer(settings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
            .Indexes({{"by_value", {"value"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync}})
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_value/indexImplTable";

        NActors::TBlockEvents<NChangeExchange::TEvChangeExchange::TEvEnqueueRecords> blockedEnqueueRecords(runtime);

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 0, .WriteTxId = 0, .Key = 1, .Value = 11},
        });
        UNIT_ASSERT_VALUES_EQUAL(blockedEnqueueRecords.size(), 1u);

        using TEvResult = NKikimrTxDataShard::TEvApplyReplicationChangesResult;
        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 22},
        }, TEvResult::STATUS_REJECTED, TEvResult::REASON_OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 11\n");

        blockedEnqueueRecords.Stop().Unblock();
        WaitForContent(server, indexPath, "value = 11, key = 1");
        SimulateSleep(server, TDuration::Seconds(1));

        ApplyChanges(server, shard, tableId, "source", {
            TChange{.Offset = 1, .WriteTxId = 0, .Key = 1, .Value = 22},
        });
        WaitForContent(server, indexPath, "value = 22, key = 1");
    }

}

} // namespace NKikimr
