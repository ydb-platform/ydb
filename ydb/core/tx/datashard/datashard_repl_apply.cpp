#include "datashard_impl.h"

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
        Y_UNUSED(ctx);

        if (Self->State != TShardState::Ready && !Self->IsReplicated()) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_WRONG_STATE);
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
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_SCHEME_ERROR,
                std::move(error));
            return true;
        }

        auto& source = EnsureSource(txc, fullTableId.PathId, msg.GetSource());

        for (const auto& change : msg.GetChanges()) {
            if (!ApplyChange(txc, fullTableId, userTable, source, change)) {
                Y_VERIFY(Result);
                break;
            }
        }

        if (MvccReadWriteVersion) {
            Pipeline.AddCommittingOp(*MvccReadWriteVersion);
        }

        if (!Result) {
            Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_OK);
        }

        return true;
    }

    TReplicationSourceState& EnsureSource(TTransactionContext& txc, const TPathId& pathId, const TString& sourceName) {
        TReplicationSourceOffsetsDb rdb(txc);
        auto* table = Self->EnsureReplicatedTable(pathId);
        Y_VERIFY(table);
        return table->EnsureSource(rdb, sourceName);
    }

    bool ApplyChange(
            TTransactionContext& txc, const TTableId& tableId, const TUserTable& userTable,
            TReplicationSourceState& source, const NKikimrTxDataShard::TEvApplyReplicationChanges::TChange& change)
    {
        Y_VERIFY(userTable.IsReplicated());

        // TODO: check source and offset, persist new values
        i64 sourceOffset = change.GetSourceOffset();

        ui64 writeTxId = change.GetWriteTxId();
        if (userTable.ReplicationConfig.HasWeakConsistency()) {
            if (writeTxId) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    "WriteTxId cannot be specified for weak consistency");
                return false;
            }
        } else {
            if (writeTxId == 0) {
                Result = MakeHolder<TEvDataShard::TEvApplyReplicationChangesResult>(
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::STATUS_REJECTED,
                    NKikimrTxDataShard::TEvApplyReplicationChangesResult::REASON_BAD_REQUEST,
                    "Non-zero WriteTxId must be specified for strong consistency");
                return false;
            }
        }

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
            key.emplace_back(keyCellVec.GetCells()[i].AsRef(), userTable.KeyColumnTypes[i]);
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
            txc.DB.UpdateTx(userTable.LocalTid, rop, key, update, writeTxId);
        } else {
            if (!MvccReadWriteVersion) {
                auto [readVersion, writeVersion] = Self->GetReadWriteVersions();
                Y_VERIFY_DEBUG(readVersion == writeVersion);
                MvccReadWriteVersion = writeVersion;
            }

            Self->SysLocksTable().BreakLocks(tableId, keyCellVec.GetCells());
            txc.DB.Update(userTable.LocalTid, rop, key, update, *MvccReadWriteVersion);
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
            update.emplace_back(tag, NTable::ECellOp::Set, TRawTypeValue(updateCellVec.GetCells()[i].AsRef(), it->second.Type));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Y_VERIFY(Ev);
        Y_VERIFY(Result);

        if (MvccReadWriteVersion) {
            Pipeline.RemoveCommittingOp(*MvccReadWriteVersion);
            Self->SendImmediateWriteResult(*MvccReadWriteVersion, Ev->Sender, Result.Release(), Ev->Cookie);
        } else {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }
    }

private:
    TPipeline& Pipeline;
    TEvDataShard::TEvApplyReplicationChanges::TPtr Ev;
    THolder<TEvDataShard::TEvApplyReplicationChangesResult> Result;
    std::optional<TRowVersion> MvccReadWriteVersion;
}; // TTxApplyReplicationChanges

void TDataShard::Handle(TEvDataShard::TEvApplyReplicationChanges::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxApplyReplicationChanges(this, Pipeline, std::move(ev)), ctx);
}

} // NDataShard
} // NKikimr
