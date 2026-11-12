#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "const.h"
#include "datashard_active_transaction.h"
#include "datashard_ut_common_kqp.h"
#include "datashard_user_table.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/string/strip.h>

namespace NKikimr {

using namespace NKikimr::NDataShard;
using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

    // A single-row batch applying `value` to the column with the given tag of a replicated table.
    template <typename T>
    THolder<TEvDataShard::TEvApplyReplicationChanges> MakeApplyRequest(
            const TTableId& tableId, ui64 sourceOffset, ui32 tag, T value)
    {
        auto request = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>(tableId.PathId, tableId.SchemaVersion);
        request->Record.SetSource("source");
        request->Record.SetDatabaseName("/Root");

        auto* change = request->Record.AddChanges();
        change->SetSourceOffset(sourceOffset);
        TCell keyCell = TCell::Make<ui32>(1);
        change->SetKey(TSerializedCellVec::Serialize({&keyCell, 1}));

        auto* upsert = change->MutableUpsert();
        upsert->AddTags(tag);
        TCell valueCell = TCell::Make<T>(value);
        upsert->SetData(TSerializedCellVec::Serialize({&valueCell, 1}));

        return request;
    }

    // Table with `value` defaulting from the table-local sequence `seq`.
    TShardedTableOptions SequenceTableOptions(const TString& valueType = "Uint32") {
        return TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"value", valueType, false, false, {}, "seq"},
            })
            .Sequences(true)
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row);
    }

} // anonymous namespace

Y_UNIT_TEST_SUITE(DataShardReplication) {

    Y_UNIT_TEST_TWIN(IncrementalRestoreConfigSurvivesSchemaAlter, WeakConsistency) {
        NKikimrSchemeOp::TTableDescription schema;
        schema.SetName("table-1");
        schema.SetPath("/Root/table-1");
        auto& config = *schema.MutableIncrementalBackupConfig();
        config.SetMode(NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP);
        config.SetConsistency(WeakConsistency
            ? NKikimrSchemeOp::TTableIncrementalBackupConfig::CONSISTENCY_WEAK
            : NKikimrSchemeOp::TTableIncrementalBackupConfig::CONSISTENCY_STRONG);
        const TUserTable original(1, schema, 0);

        NKikimrSchemeOp::TTableDescription delta;
        delta.SetName("table-1");
        delta.MutableIncrementalBackupConfig()->CopyFrom(config);
        const TUserTable altered(original, delta);
        NKikimrSchemeOp::TTableDescription persisted;
        altered.GetSchema(persisted);
        UNIT_ASSERT(persisted.HasIncrementalBackupConfig());
        UNIT_ASSERT(!persisted.HasReplicationConfig());

        const TUserTable restored(1, persisted, 0);
        UNIT_ASSERT(restored.IsIncrementalRestore());
        UNIT_ASSERT(!restored.IsReplicated());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(restored.IncrementalBackupConfig.Consistency),
            static_cast<ui32>(config.GetConsistency()));
    }

    Y_UNIT_TEST_TWIN(AttachedReplicationSurvivesRestart, GlobalConsistency) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 10);");

        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        tx.SetWorkingDir("/Root");
        tx.SetInternal(true);
        auto& desc = *tx.MutableAlterTable();
        desc.SetName("table-1");
        auto& config = *desc.MutableReplicationConfig();
        config.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        config.SetConsistencyLevel(GlobalConsistency
            ? NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_GLOBAL
            : NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
        const auto altered = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        UNIT_ASSERT_VALUES_EQUAL(altered->Get()->Record.GetStatus(),
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
        WaitTxNotification(server, sender, altered->Get()->Record.GetTxId());

        const auto shards = GetTableShards(server, sender, "/Root/table-1");
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        UNIT_ASSERT(tableId.SchemaVersion);
        const auto apply = [&](i64 offset, ui32 value) {
            const ui64 writeTxId = GlobalConsistency ? 123 + offset : 0;
            ApplyChanges(server, shards.at(0), tableId, "my-source", {
                TChange{ .Offset = offset, .WriteTxId = writeTxId,
                    .Key = 1, .Value = value },
            });
            if (GlobalConsistency) {
                CommitWrites(server, {"/Root/table-1"}, writeTxId);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                TStringBuilder() << "key = 1, value = " << value << "\n");
        };
        apply(0, 11);
        RebootTablet(runtime, shards.at(0), sender);
        apply(1, 12);

        WaitTxNotification(server, sender, AsyncAlterDropReplicationConfig(server, "/Root", "table-1"));
        RebootTablet(runtime, shards.at(0), sender);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 13);");
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 13\n");
        ApplyChanges(server, shards.at(0), ResolveTableId(server, sender, "/Root/table-1"), "my-source", {
            TChange{ .Offset = 2, .WriteTxId = 0, .Key = 1, .Value = 14 },
        }, NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
            NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST);
    }

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

    Y_UNIT_TEST_TWIN(RejectedChangeDoesNotAdvanceOffset, Reboot) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row)
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        using TChangeProto = NKikimrTxDataShard::TEvApplyReplicationChanges::TChange;
        using TEvResult = NKikimrTxDataShard::TEvApplyReplicationChangesResult;
        const auto makeChange = [](i64 offset, ui32 key, ui32 value) {
            TChangeProto change;
            change.SetSourceOffset(offset);
            const TCell keyCell = TCell::Make(key);
            change.SetKey(TSerializedCellVec::Serialize({&keyCell, 1}));
            auto* upsert = change.MutableUpsert();
            upsert->AddTags(2);
            const TCell valueCell = TCell::Make(value);
            upsert->SetData(TSerializedCellVec::Serialize({&valueCell, 1}));
            return change;
        };
        const auto apply = [&](const TVector<TChangeProto>& changes, TEvResult::EStatus status,
                TEvResult::EReason reason = TEvResult::REASON_NONE) {
            auto request = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>(tableId.PathId, tableId.SchemaVersion);
            request->Record.SetSource("source");
            for (const auto& change : changes) {
                request->Record.AddChanges()->CopyFrom(change);
            }
            const auto replyTo = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, replyTo, request.Release(), 0, GetPipeConfigWithRetries());
            const auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(replyTo);
            const auto& record = result->Get()->Record;
            UNIT_ASSERT_C(record.GetStatus() == status,
                record.ShortDebugString() << ", expected " << TEvResult::EStatus_Name(status));
            UNIT_ASSERT_C(record.GetReason() == reason,
                record.ShortDebugString() << ", expected " << TEvResult::EReason_Name(reason));
        };

        TVector<std::pair<TString, TChangeProto>> invalidChanges;
        for (bool reset : {false, true}) {
            const TString operation = reset ? "reset" : "upsert";
            auto change = makeChange(1, 2, 22);
            if (reset) {
                const auto updates = change.GetUpsert();
                change.MutableReset()->CopyFrom(updates);
            }
            auto* updates = reset ? change.MutableReset() : change.MutableUpsert();
            updates->SetTags(0, 3);
            invalidChanges.emplace_back(operation + " unknown column", change);
            updates->SetTags(0, 1);
            invalidChanges.emplace_back(operation + " primary key column", change);
            updates->SetTags(0, 2);
            updates->AddTags(2);
            invalidChanges.emplace_back(operation + " tag count mismatch", change);
            updates->ClearTags();
            updates->AddTags(2);
            updates->SetData("\x01");
            invalidChanges.emplace_back(operation + " malformed data", change);
        }
        auto missingOperation = makeChange(1, 2, 22);
        missingOperation.ClearUpsert();
        invalidChanges.emplace_back("missing operation", missingOperation);

        TString expected;
        ui32 firstKey = 1;
        for (auto& [name, invalid] : invalidChanges) {
            const i64 firstOffset = firstKey - 1;
            const auto prefix = makeChange(firstOffset, firstKey, 11);
            const auto corrected = makeChange(firstOffset + 1, firstKey + 1, 22);
            const auto suffix = makeChange(firstOffset + 2, firstKey + 2, 33);
            invalid.SetSourceOffset(corrected.GetSourceOffset());
            invalid.SetKey(corrected.GetKey());

            // A rejected record commits the valid prefix, but neither itself nor the suffix.
            const auto reason = invalid.RowOperation_case() == TChangeProto::ROWOPERATION_NOT_SET
                ? TEvResult::REASON_UNEXPECTED_ROW_OPERATION : TEvResult::REASON_BAD_REQUEST;
            apply({prefix, invalid, suffix}, TEvResult::STATUS_REJECTED, reason);
            expected += TStringBuilder() << "key = " << firstKey << ", value = 11\n";
            UNIT_ASSERT_VALUES_EQUAL_C(ReadShardedTable(server, "/Root/table-1"), expected, name);

            if (Reboot) {
                RebootTablet(runtime, shard, sender);
            }

            // Reuse the rejected offset and replay the prefix with a different value.
            apply({makeChange(firstOffset, firstKey, 99), corrected, suffix}, TEvResult::STATUS_OK);
            expected += TStringBuilder() << "key = " << firstKey + 1 << ", value = 22\n"
                << "key = " << firstKey + 2 << ", value = 33\n";
            UNIT_ASSERT_VALUES_EQUAL_C(ReadShardedTable(server, "/Root/table-1"), expected, name);

            // Previously accepted offsets remain duplicates, even with invalid payloads.
            apply({invalid}, TEvResult::STATUS_OK);
            UNIT_ASSERT_VALUES_EQUAL_C(ReadShardedTable(server, "/Root/table-1"), expected, name);
            firstKey += 3;
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

    Y_UNIT_TEST_QUAD(AsyncIndexOversizedSnapshotReplay, GlobalConsistency, Reboot) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"a", "String", false, false},
                {"b", "String", false, false},
                {"payload", "String", false, false},
            })
            .Replicated(true)
            .ReplicationConsistencyLevel(GlobalConsistency ? EConsistencyLevel::Global : EConsistencyLevel::Row)
            .Indexes({
                {"by_ab", {"a", "b"}, {"payload"}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
                {"by_payload", {"payload"}, {}, NKikimrSchemeOp::EIndexTypeGlobalAsync},
            })
        );

        const auto shard = GetTableShards(server, sender, "/Root/table-1").at(0);
        const auto tableId = ResolveTableId(server, sender, "/Root/table-1");
        const TString indexPath = "/Root/table-1/by_ab/indexImplTable";
        const TString otherIndexPath = "/Root/table-1/by_payload/indexImplTable";
        const TString large(630000, 'L');
        using TChangeProto = NKikimrTxDataShard::TEvApplyReplicationChanges::TChange;
        using TEvResult = NKikimrTxDataShard::TEvApplyReplicationChangesResult;

        const auto makeChange = [&](i64 offset, ui32 key, const TVector<std::pair<ui32, TString>>& values, bool erase = false) {
            TChangeProto change;
            change.SetSourceOffset(offset);
            change.SetWriteTxId(GlobalConsistency ? 100 + offset : 0);
            const TCell keyCell = TCell::Make(key);
            change.SetKey(TSerializedCellVec::Serialize({&keyCell, 1}));
            if (erase) {
                change.MutableErase();
            } else {
                auto* upsert = change.MutableUpsert();
                TVector<TCell> cells;
                for (const auto& [tag, value] : values) {
                    upsert->AddTags(tag);
                    cells.emplace_back(value.data(), value.size());
                }
                upsert->SetData(TSerializedCellVec::Serialize(cells));
            }
            return change;
        };
        const auto apply = [&](const TVector<TChangeProto>& changes, bool reject = false) {
            auto request = MakeHolder<TEvDataShard::TEvApplyReplicationChanges>(tableId.PathId, tableId.SchemaVersion);
            request->Record.SetSource("source");
            for (const auto& change : changes) {
                request->Record.AddChanges()->CopyFrom(change);
            }
            const auto replyTo = runtime.AllocateEdgeActor();
            runtime.SendToPipe(shard, replyTo, request.Release(), 0, GetPipeConfigWithRetries());
            const auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(replyTo);
            const auto& record = result->Get()->Record;
            UNIT_ASSERT_C(record.GetStatus() == (reject ? TEvResult::STATUS_REJECTED : TEvResult::STATUS_OK),
                record.ShortDebugString());
            UNIT_ASSERT_C(record.GetReason() == (reject ? TEvResult::REASON_BAD_REQUEST : TEvResult::REASON_NONE),
                record.ShortDebugString());
        };
        const auto commit = [&](i64 offset) {
            if (GlobalConsistency) {
                CommitWrites(server, {"/Root/table-1"}, 100 + offset);
            }
        };
        const auto reboot = [&] {
            if (Reboot) {
                RebootTablet(runtime, shard, sender);
            }
        };

        // The restored snapshot already contains the later source state (S, L).
        apply({makeChange(0, 1, {{2, "S"}, {3, large}, {4, "snapshot"}})});
        commit(0);
        const TString snapshotIndex = TStringBuilder() << "a = S, b = " << large << ", key = 1, payload = snapshot";
        WaitForContent(server, indexPath, snapshotIndex);

        // Explicit oversized keys must still reject the entire batch, including
        // its valid prefix and source offsets, even with an existing pre-image.
        apply({makeChange(1, 1, {{4, "rejected"}}), makeChange(2, 1, {{2, large}, {3, large}})}, true);
        apply({makeChange(1, 1, {{2, TString(NLimits::MaxWriteKeySize + 1, 'X')}})}, true);
        reboot();
        WaitForContent(server, indexPath, snapshotIndex);
        WaitForContent(server, otherIndexPath, "payload = snapshot, key = 1");

        // An older partial update creates (L, L) only on the destination.
        apply({makeChange(1, 1, {{2, large}})});
        WaitForContent(server, indexPath, "");
        reboot();

        // Covered-column changes and other indexes must continue to work while
        // this key is omitted. GLOBAL writes are still uncommitted at this point.
        apply({makeChange(2, 1, {{4, "intermediate"}})});
        WaitForContent(server, otherIndexPath, "payload = intermediate, key = 1");
        WaitForContent(server, indexPath, "");
        if (!GlobalConsistency) {
            UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
                TStringBuilder() << "key = 1, a = " << large << ", b = " << large << ", payload = intermediate\n");
        }
        reboot();

        // A partial update restores a valid key without deleting the oversized
        // one. Duplicate delivery must not revert the covered columns.
        apply({makeChange(3, 1, {{2, "S"}, {4, "recovered"}})});
        apply({makeChange(2, 1, {{4, "duplicate"}})});
        const TString recoveredIndex = TStringBuilder() << "a = S, b = " << large << ", key = 1, payload = recovered";
        WaitForContent(server, indexPath, recoveredIndex);
        WaitForContent(server, otherIndexPath, "payload = recovered, key = 1");
        for (i64 offset = 1; offset <= 3; ++offset) {
            commit(offset);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
            TStringBuilder() << "key = 1, a = S, b = " << large << ", payload = recovered\n");

        // Deleting an omitted key must also work after restart. A following row
        // proves that no oversized deletion has blocked the index sender.
        apply({makeChange(4, 1, {{2, large}})});
        WaitForContent(server, indexPath, "");
        reboot();
        apply({makeChange(5, 1, {}, true), makeChange(6, 2, {{2, "M"}, {3, "M"}, {4, "marker"}})});
        WaitForContent(server, indexPath, "a = M, b = M, key = 2, payload = marker");
        WaitForContent(server, otherIndexPath, "payload = marker, key = 2");
        for (i64 offset = 4; offset <= 6; ++offset) {
            commit(offset);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"),
            "key = 2, a = M, b = M, payload = marker\n");
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

    // The batch must not be applied until the sequence has been advanced past its values: the
    // data shard sends the advance first and waits for the result.
    Y_UNIT_TEST(ApplyWaitsForSequenceAdvance) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", SequenceTableOptions());

        TBlockEvents<NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequence> blocked(runtime);

        const auto requester = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shards.at(0), requester,
            MakeApplyRequest(tableId, 1, 2, ui32(42)).Release(), 0, GetPipeConfigWithRetries());

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1u);
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvDataShard::TEvApplyReplicationChangesResult>(
            requester, TDuration::Seconds(1)));

        const auto advance = blocked.front();
        const auto advancePath = std::get<TString>(advance->Get()->Path);
        UNIT_ASSERT_VALUES_EQUAL(advancePath, "/Root/table-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(advance->Get()->ObservedMin, 42);
        UNIT_ASSERT_VALUES_EQUAL(advance->Get()->ObservedMax, 42);

        const auto advanceSender = advance->Sender;
        const auto advanceCookie = advance->Cookie;

        blocked.Stop();
        blocked.clear();

        runtime.Send(new IEventHandle(advanceSender, requester,
            new NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequenceResult(tableId.PathId, 42, true),
            0, advanceCookie));

        auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(requester);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result->Get()->Record.GetStatus()),
            static_cast<ui32>(NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK));
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 42\n");
    }

    void DoTestApplyAdvancesSequence(const TString& type) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", SequenceTableOptions(type));

        const auto requester = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shards.at(0), requester, (type == "Uint32"
            ? MakeApplyRequest(tableId, 1, 2, ui32(42)).Release()
            : MakeApplyRequest(tableId, 1, 2, i16(42)).Release()), 0, GetPipeConfigWithRetries());

        auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(requester);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result->Get()->Record.GetStatus()),
            static_cast<ui32>(NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK));
        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "key = 1, value = 42\n");

        const auto nextValSender = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(NSequenceProxy::MakeSequenceProxyServiceID(0), nextValSender,
            new NSequenceProxy::TEvSequenceProxy::TEvNextVal("/Root", "/Root/table-1/seq")));

        auto nextVal = runtime.GrabEdgeEventRethrow<NSequenceProxy::TEvSequenceProxy::TEvNextValResult>(nextValSender);
        UNIT_ASSERT_VALUES_EQUAL(nextVal->Get()->Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(nextVal->Get()->Value, 43);
    }

    Y_UNIT_TEST(ApplyAdvancesSequence) {
        DoTestApplyAdvancesSequence("Uint32");
    }

    Y_UNIT_TEST(ApplyAdvancesSmallSerialSequence) {
        DoTestApplyAdvancesSequence("Int16");
    }

    // If the advance does not complete in time, the batch is rejected (and retried by the
    // replication service later) instead of being applied with a lagging sequence.
    Y_UNIT_TEST(ApplyFailsOnSequenceAdvanceTimeout) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", SequenceTableOptions());

        TBlockEvents<NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequence> blocked(runtime);

        const auto requester = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shards.at(0), requester,
            MakeApplyRequest(tableId, 1, 2, ui32(42)).Release(), 0, GetPipeConfigWithRetries());

        SimulateSleep(server, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1u);
        UNIT_ASSERT(!runtime.GrabEdgeEvent<TEvDataShard::TEvApplyReplicationChangesResult>(
            requester, TDuration::Seconds(1)));

        SimulateSleep(server, TDuration::Seconds(11));

        auto result = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvApplyReplicationChangesResult>(requester);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result->Get()->Record.GetStatus()),
            static_cast<ui32>(NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED));
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(result->Get()->Record.GetReason()),
            static_cast<ui32>(NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED));

        UNIT_ASSERT_VALUES_EQUAL(ReadShardedTable(server, "/Root/table-1"), "");
    }

    // `__ydb_row_id` is always stored with the spread layout (the query compiler applies it by column
    // name), so the backing sequence must be advanced in the dense seq space rather than in the row-id
    // space.
    Y_UNIT_TEST(ApplyAdvancesRowIdSequenceInSeqSpace) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);

        auto opts = TShardedTableOptions()
            .Columns({
                {"key", "Uint32", true, false},
                {"__ydb_row_id", "Uint64", false, false, {}, "_seq___ydb_row_id"},
            })
            .Sequences(true)
            .AllowSystemColumnNames(true)
            .Replicated(true)
            .ReplicationConsistencyLevel(EConsistencyLevel::Row);
        auto [shards, tableId] = CreateShardedTable(server, sender, "/Root", "table-1", opts);

        TBlockEvents<NSequenceProxy::TEvSequenceProxy::TEvAdvanceSequence> blocked(runtime);

        const ui64 seq = 7;
        const auto rowId = NTableIndex::NFulltext::RowIdFromSeq(seq);

        const auto requester = runtime.AllocateEdgeActor();
        runtime.SendToPipe(shards.at(0), requester,
            MakeApplyRequest(tableId, 1, /*tag=*/2, ui64(rowId)).Release(), 0, GetPipeConfigWithRetries());

        SimulateSleep(server, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1u);

        const auto advance = blocked.front();
        UNIT_ASSERT_VALUES_EQUAL(std::get<TString>(advance->Get()->Path), "/Root/table-1/_seq___ydb_row_id");
        UNIT_ASSERT_VALUES_EQUAL(advance->Get()->ObservedMin, static_cast<i64>(seq));
        UNIT_ASSERT_VALUES_EQUAL(advance->Get()->ObservedMax, static_cast<i64>(seq));

        blocked.Stop();
        blocked.clear();
    }

}

} // namespace NKikimr
