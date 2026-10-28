#include "datashard_impl.h"
#include "datashard_integrity_trails.h"
#include "datashard_tli.h"
#include "datashard_locks_db.h"
#include "datashard_user_db.h"
#include "const.h"
#include "setup_sys_locks.h"

#include <ydb/core/engine/minikql/minikql_engine_host_counters.h>

#include <util/string/escape.h>

namespace NKikimr {
namespace NDataShard {

using namespace NTabletFlatExecutor;

class TDataShard::TTxApplyReplicationChanges : public TTransactionBase<TDataShard> {
public:
    explicit TTxApplyReplicationChanges(TDataShard* self, TPipeline& pipeline,
            TEvDataShard::TEvApplyReplicationChanges::TPtr&& ev)
        : TTransactionBase(self)
        , Pipeline(pipeline)
        , Ev(std::move(ev))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_APPLY_REPLICATION_CHANGES;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Result.Reset();
        MvccVersion.reset();
        Changes.clear();
        CommittingOpRegistered = false;

        TDataShardLocksDb locksDb(*Self, txc);
        TSetupSysLocks guardLocks(*Self, &locksDb);

        if (Self->State != TShardState::Ready) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_WRONG_STATE,
                TStringBuilder() << "DataShard is not ready");
            return true;
        }

        if (!Self->IsReplicated() && !Self->IsIncrementalRestore()) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Table is nor replicated nor under incremental restore");
            return true;
        }

        const auto& msg = Ev->Get()->Record;

        const auto& tableId = msg.GetTableId();
        const TTableId fullTableId(tableId.GetOwnerId(), tableId.GetTableId());

        const auto& userTables = Self->GetUserTables();
        auto it = userTables.find(fullTableId.PathId.LocalPathId);
        if (fullTableId.PathId.OwnerId != Self->GetPathOwnerId() || it == userTables.end()) {
            TString error = TStringBuilder()
                << "DataShard " << Self->TabletID() << " does not have a table "
                << tableId.GetOwnerId() << ":" << tableId.GetTableId();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR,
                std::move(error));
            return true;
        }

        const auto& userTable = *it->second;
        if (tableId.GetSchemaVersion() != 0 && userTable.GetTableSchemaVersion() != tableId.GetSchemaVersion()) {
            TString error = TStringBuilder()
                << "DataShard " << Self->TabletID() << " has table "
                << tableId.GetOwnerId() << ":" << tableId.GetTableId()
                << " with schema version " << userTable.GetTableSchemaVersion()
                << " and cannot apply changes for schema version " << tableId.GetSchemaVersion();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                tableId.GetSchemaVersion() < userTable.GetTableSchemaVersion()
                    ? NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OUTDATED_SCHEME
                    : NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR,
                std::move(error));
            return true;
        }

        if (userTable.HasAsyncIndexes() && Self->CheckChangesQueueOverflow()) {
            Self->IncCounter(COUNTER_CHANGE_QUEUE_OVERFLOW_REJECTS);
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_OVERLOADED,
                "Change queue overflow");
            return true;
        }

        auto* replicatedTable = Self->EnsureReplicatedTable(fullTableId.PathId);
        Y_ENSURE(replicatedTable);
        const bool sourceCreated = !replicatedTable->Sources.contains(msg.GetSource());
        const ui64 savedNextSourceId = replicatedTable->NextSourceId;
        TReplicationSourceOffsetsDb rdb(txc);
        auto& source = replicatedTable->EnsureSource(rdb, msg.GetSource());

        if (userTable.HasAsyncIndexes()) {
            // Index collection may fault while reading the old row. Restore the
            // in-memory source state together with the database transaction.
            const ui64 sourceId = source.Id;
            const TString sourceName = source.Name;
            auto savedOffsets = source.OffsetBySplitKeyId;
            const ui64 savedNextSplitKeyId = source.NextSplitKeyId;
            const ui64 savedStatBytes = source.StatBytes;
            txc.DB.OnRollback([replicatedTable, &source, sourceCreated, sourceId, sourceName, savedNextSourceId,
                    savedOffsets = std::move(savedOffsets), savedNextSplitKeyId, savedStatBytes]() mutable
            {
                if (sourceCreated) {
                    replicatedTable->Sources.erase(sourceName);
                    replicatedTable->SourceById.erase(sourceId);
                    if (replicatedTable->NextSourceId == sourceId + 1) {
                        replicatedTable->NextSourceId = savedNextSourceId;
                    }
                    return;
                }
                source.Offsets.clear();
                source.OffsetBySplitKeyId = std::move(savedOffsets);
                for (auto& [_, state] : source.OffsetBySplitKeyId) {
                    source.Offsets.insert(&state);
                }
                source.NextSplitKeyId = savedNextSplitKeyId;
                source.StatBytes = savedStatBytes;
            });
        }

        NMiniKQL::TEngineHostCounters counters;
        TDataShardUserDb userDb(*Self, txc.DB, 0, Self->GetMvccVersion(), counters, ctx.Now());
        TDataShardChangeGroupProvider groupProvider(*Self, txc.DB);
        THolder<IDataShardChangeCollector> collector;
        if (userTable.HasAsyncIndexes()) {
            collector.Reset(CreateChangeCollector(*Self, userDb, groupProvider, txc.DB, userTable));
            // Previously staged base writes are the logical pre-image for new
            // global-consistency stream records, including after a shard reboot.
            for (ui64 openTxId : txc.DB.GetOpenTxs(userTable.LocalTid)) {
                userDb.AddCommitTxId(fullTableId, openTxId);
            }
        }

        try {
            for (const auto& change : msg.GetChanges()) {
                if (!ApplyChange(txc, fullTableId, userTable, source, change, userDb, collector.Get())) {
                    Y_ENSURE(Result);
                    break;
                }
            }
        } catch (const TNotReadyTabletException&) {
            txc.Reschedule();
            return false;
        } catch (const TKeySizeConstraintException&) {
            // Reject the whole batch, including previously applied rows and offsets.
            txc.DB.RollbackChanges();
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Size of key in secondary index is more than " << NLimits::MaxWriteKeySize);
            return true;
        }

        if (collector) {
            Changes = std::move(collector->GetCollected());
        }

        if (MvccVersion) {
            Self->PromoteImmediatePostExecuteEdges(*MvccVersion, TDataShard::EPromotePostExecuteEdges::ReadWrite, txc);
            Pipeline.AddCommittingOp(*MvccVersion);
            CommittingOpRegistered = true;
        }

        if (!Result) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK);
        }

        auto [_, locksBrokenByReplication] = Self->SysLocksTable().ApplyLocks();
        if (!locksBrokenByReplication.empty()) {
            auto victimQuerySpanIds = Self->SysLocksTable().ExtractVictimQuerySpanIds(locksBrokenByReplication);
            NDataIntegrity::LogLocksBroken(ctx, Self->TabletID(), "Replication apply broke locks on replicated rows", locksBrokenByReplication,
                                           Nothing(), victimQuerySpanIds);
        }
        return true;
    }

    bool ApplyChange(
            TTransactionContext& txc, const TTableId& tableId, const TUserTable& userTable,
            TReplicationSourceState& source, const NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& change,
            TDataShardUserDb& userDb, IDataShardChangeCollector* collector)
    {
        Y_ENSURE(userTable.IsReplicated() || Self->IsIncrementalRestore());

        // TODO: check source and offset, persist new values
        i64 sourceOffset = change.GetSourceOffset();

        ui64 writeTxId = change.GetWriteTxId();
        if (userTable.ReplicationConfig.HasRowConsistency() || userTable.IncrementalBackupConfig.HasWeakConsistency()) {
            if (writeTxId) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    "WriteTxId cannot be specified for row consistency");
                return false;
            }
        }

        // Global replicas also accept immediate writes during index initial
        // scan. The replication protocol chooses when to use WriteTxId.

        TSerializedCellVec keyCellVec;
        if (!TSerializedCellVec::TryParse(change.GetKey(), keyCellVec) ||
            keyCellVec.GetCells().size() != userTable.KeyColumnTypes.size())
        {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Key at " << EscapeC(source.Name) << ":" << sourceOffset << " is not a valid primary key");
            return false;
        }

        TReplicationSourceOffsetsDb rdb(txc);
        if (!source.AdvanceMaxOffset(rdb, keyCellVec.GetCells(), sourceOffset)) {
            // We have already seen this offset and ignore it
            return true;
        }

        TVector<TRawTypeValue> key;
        key.reserve(keyCellVec.GetCells().size());
        for (size_t i = 0; i < keyCellVec.GetCells().size(); ++i) {
            key.emplace_back(keyCellVec.GetCells()[i].AsRef(), userTable.KeyColumnTypes[i].GetTypeId());
        }

        NTable::ERowOp rop = NTable::ERowOp::Absent;
        TSerializedCellVec updateCellVec;
        TVector<NTable::TUpdateOp> update;
        switch (change.RowOperation_case()) {
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kUpsert: {
                rop = NTable::ERowOp::Upsert;
                if (!ParseUpdatesProto(userTable, source, sourceOffset, change.GetUpsert(), updateCellVec, update)) {
                    return false;
                }
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kErase: {
                rop = NTable::ERowOp::Erase;
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::kReset: {
                rop = NTable::ERowOp::Reset;
                if (!ParseUpdatesProto(userTable, source, sourceOffset, change.GetReset(), updateCellVec, update)) {
                    return false;
                }
                break;
            }
            case NKikimrTxDataShard::TEvApplyReplicationChanges::TChange::ROWOPERATION_NOT_SET: {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_UNEXPECTED_ROW_OPERATION,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " has an unexpected row operation");
                return false;
            }
        }

        if (writeTxId) {
            if (collector) {
                if (!MvccVersion) {
                    MvccVersion = Self->GetMvccVersion();
                }
                if (!collector->OnUpdate(tableId, userTable.LocalTid, rop, key, update, *MvccVersion, nullptr)) {
                    throw TNotReadyTabletException();
                }
            }
            txc.DB.UpdateTx(userTable.LocalTid, rop, key, update, writeTxId);
            if (collector) {
                userDb.AddCommitTxId(tableId, writeTxId);
            }
            Self->GetConflictsCache().GetTableCache(userTable.LocalTid).AddUncommittedWrite(keyCellVec.GetCells(), writeTxId, txc.DB);
        } else {
            if (!MvccVersion) {
                MvccVersion = Self->GetMvccVersion();
            }

            Self->SysLocksTable().BreakLocks(tableId, keyCellVec.GetCells());
            if (collector && !collector->OnUpdate(tableId, userTable.LocalTid, rop, key, update, *MvccVersion, nullptr)) {
                throw TNotReadyTabletException();
            }
            txc.DB.Update(userTable.LocalTid, rop, key, update, *MvccVersion);
            Self->GetConflictsCache().GetTableCache(userTable.LocalTid).RemoveUncommittedWrites(keyCellVec.GetCells(), txc.DB);
        }

        return true;
    }

    bool ParseUpdatesProto(
            const TUserTable& userTable,
            TReplicationSourceState& source, ui64 sourceOffset,
            const NKikimrTxDataShard::TEvApplyReplicationChanges::TUpdates& proto,
            TSerializedCellVec& updateCellVec,
            TVector<NTable::TUpdateOp>& update)
    {
        const auto& tags = proto.GetTags();
        size_t count = tags.size();
        if (!TSerializedCellVec::TryParse(proto.GetData(), updateCellVec) ||
            updateCellVec.GetCells().size() != count)
        {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " has invalid data");
            return false;
        }
        update.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            ui32 tag = tags[i];
            auto it = userTable.Columns.find(tag);
            if (it == userTable.Columns.end()) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " is updating an unknown column " << tag);
                return false;
            }
            if (it->second.IsKey) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    TStringBuilder() << "Update at " << EscapeC(source.Name) << ":" << sourceOffset << " is updating a primary key column " << tag);
                return false;
            }
            update.emplace_back(tag, NTable::ECellOp::Set, TRawTypeValue(updateCellVec.GetCells()[i].AsRef(), it->second.Type.GetTypeId()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_ENSURE(Ev);
        Y_ENSURE(Result);

        if (CommittingOpRegistered) {
            Y_ENSURE(MvccVersion);
            Pipeline.RemoveCommittingOp(*MvccVersion);
            Self->SendImmediateWriteResult(*MvccVersion, Ev->Sender, Result.Release(), Ev->Cookie);
        } else {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Changes) {
            Self->EnqueueChangeRecords(std::move(Changes));
        }
    }

private:
    TPipeline& Pipeline;
    TEvDataShard::TEvApplyReplicationChanges::TPtr Ev;
    THolder<TEvDataShard::TEvApplyReplicationChangesResult> Result;
    std::optional<TRowVersion> MvccVersion;
    bool CommittingOpRegistered = false;
    TVector<IDataShardChangeCollector::TChange> Changes;
}; // TTxApplyReplicationChanges

void TDataShard::Handle(TEvDataShard::TEvApplyReplicationChanges::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxApplyReplicationChanges(this, Pipeline, std::move(ev)), ctx);
}

} // NDataShard
} // NKikimr
