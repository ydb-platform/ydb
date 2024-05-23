#include "change_collector.h"
#include "datashard_common_upload.h"
#include "datashard_user_db.h"

namespace NKikimr::NDataShard {

template <typename TEvRequest, typename TEvResponse>
TCommonUploadOps<TEvRequest, TEvResponse>::TCommonUploadOps(typename TEvRequest::TPtr& ev, bool breakLocks, bool collectChanges)
    : Ev(ev)
    , BreakLocks(breakLocks)
    , CollectChanges(collectChanges)
{
}

template <typename TEvRequest, typename TEvResponse>
bool TCommonUploadOps<TEvRequest, TEvResponse>::Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& readVersion, const TRowVersion& writeVersion, ui64 globalTxId,
        absl::flat_hash_set<ui64>* volatileReadDependencies)
{
    const auto& record = Ev->Get()->Record;
    Result = MakeHolder<TEvResponse>(self->TabletID());

    TInstant deadline = TInstant::MilliSeconds(record.GetCancelDeadlineMs());
    if (deadline && deadline < AppData()->TimeProvider->Now()) {
        SetError(NKikimrTxDataShard::TError::EXECUTION_CANCELLED, "Deadline exceeded");
        return true;
    }

    const ui64 tableId = record.GetTableId();
    const TTableId fullTableId(self->GetPathOwnerId(), tableId);
    const ui64 localTableId = self->GetLocalTableId(fullTableId);
    if (localTableId == 0) {
        SetError(NKikimrTxDataShard::TError::SCHEME_ERROR, Sprintf("Unknown table id %" PRIu64, tableId));
        return true;
    }
    const ui64 shadowTableId = self->GetShadowTableId(fullTableId);

    const TUserTable& tableInfo = *self->GetUserTables().at(tableId); /// ... find
    Y_ABORT_UNLESS(tableInfo.LocalTid == localTableId);
    Y_ABORT_UNLESS(tableInfo.ShadowTid == shadowTableId);

    // Check schemas
    if (record.GetRowScheme().KeyColumnIdsSize() != tableInfo.KeyColumnIds.size()) {
        SetError(NKikimrTxDataShard::TError::SCHEME_ERROR,
            Sprintf("Key column count mismatch: got %" PRIu64 ", expected %" PRIu64,
                record.GetRowScheme().KeyColumnIdsSize(), tableInfo.KeyColumnIds.size()));
        return true;
    }

    if (record.GetSchemaVersion() && tableInfo.GetTableSchemaVersion() &&
        record.GetSchemaVersion() != tableInfo.GetTableSchemaVersion())
    {
        SetError(NKikimrTxDataShard::TError::SCHEME_ERROR, TStringBuilder()
            << "Schema version mismatch"
            << ": requested " << record.GetSchemaVersion()
            << ", expected " << tableInfo.GetTableSchemaVersion()
            << ". Retry request with an updated schema.");
        return true;
    }

    for (size_t i = 0; i < tableInfo.KeyColumnIds.size(); ++i) {
        if (record.GetRowScheme().GetKeyColumnIds(i) != tableInfo.KeyColumnIds[i]) {
            SetError(NKikimrTxDataShard::TError::SCHEME_ERROR, Sprintf("Key column schema at position %" PRISZT, i));
            return true;
        }
    }

    const bool upsertIfExists = record.GetUpsertIfExists();
    const bool writeToTableShadow = record.GetWriteToTableShadow();
    const bool readForTableShadow = writeToTableShadow && !shadowTableId;
    const ui32 writeTableId = writeToTableShadow && shadowTableId ? shadowTableId : localTableId;

    const bool breakWriteConflicts = BreakLocks && (
        self->SysLocksTable().HasWriteLocks(fullTableId) ||
        self->GetVolatileTxManager().GetTxMap());

    NMiniKQL::TEngineHostCounters engineHostCounters;
    TDataShardUserDb userDb(*self, txc.DB, globalTxId, readVersion, writeVersion, engineHostCounters, TAppData::TimeProvider->Now());
    TDataShardChangeGroupProvider groupProvider(*self, txc.DB);

    if (CollectChanges) {
        ChangeCollector.Reset(CreateChangeCollector(*self, userDb, groupProvider, txc.DB, tableInfo));
    }

    // Prepare (id, Type) vector for value columns
    TVector<NTable::TTag> tagsForSelect;
    TVector<std::pair<ui32, NScheme::TTypeInfo>> valueCols;
    for (const auto& colId : record.GetRowScheme().GetValueColumnIds()) {
        if (readForTableShadow) {
            tagsForSelect.push_back(colId);
        }
        auto* col = tableInfo.Columns.FindPtr(colId);
        if (!col) {
            SetError(NKikimrTxDataShard::TError::SCHEME_ERROR, Sprintf("Missing column with id=%" PRIu32, colId));
            return true;
        }
        valueCols.emplace_back(colId, col->Type);
    }

    TVector<TRawTypeValue> key;
    TVector<NTable::TUpdateOp> value;

    TSerializedCellVec keyCells;
    TSerializedCellVec valueCells;

    bool pageFault = false;
    bool commitAdded = false;
    NTable::TRowState rowState;

    absl::flat_hash_set<ui64> volatileDependencies;

    ui64 bytes = 0;
    for (const auto& r : record.GetRows()) {
        // TODO: use safe parsing!
        keyCells.Parse(r.GetKeyColumns());
        valueCells.Parse(r.GetValueColumns());

        bytes += keyCells.GetBuffer().size() + valueCells.GetBuffer().size();

        if (keyCells.GetCells().size() != tableInfo.KeyColumnTypes.size() ||
            valueCells.GetCells().size() != valueCols.size())
        {
            SetError(NKikimrTxDataShard::TError::SCHEME_ERROR, "Cell count doesn't match row scheme");
            return true;
        }

        key.clear();
        size_t ki = 0;
        ui64 keyBytes = 0;
        for (const auto& kt : tableInfo.KeyColumnTypes) {
            const TCell& c = keyCells.GetCells()[ki];
            if (kt.GetTypeId() == NScheme::NTypeIds::Uint8 && !c.IsNull() && c.AsValue<ui8>() > 127) {
                SetError(NKikimrTxDataShard::TError::BAD_ARGUMENT, "Keys with Uint8 column values >127 are currently prohibited");
                return true;
            }

            keyBytes += c.Size();
            key.emplace_back(TRawTypeValue(c.AsRef(), kt));
            ++ki;
        }

        if (keyBytes > NLimits::MaxWriteKeySize) {
            SetError(NKikimrTxDataShard::TError::BAD_ARGUMENT,
                     Sprintf("Row key size of %" PRISZT " bytes is larger than the allowed threshold %" PRIu64,
                             keyBytes, NLimits::MaxWriteKeySize));
            return true;
        }

        if (readForTableShadow) {
            rowState.Init(tagsForSelect.size());

            auto ready = txc.DB.Select(localTableId, key, tagsForSelect, rowState, 0 /* readFlags */, readVersion);
            if (ready == NTable::EReady::Page) {
                pageFault = true;
            }

            if (pageFault) {
                continue;
            }

            if (rowState == NTable::ERowOp::Erase || rowState == NTable::ERowOp::Reset) {
                // Row has been erased in the past, ignore this upsert
                continue;
            }
        }

        if (upsertIfExists) {
            rowState.Init(tagsForSelect.size());
            auto ready = userDb.SelectRow(fullTableId, key, tagsForSelect, rowState);
            if (ready == NTable::EReady::Page) {
                pageFault = true;
            }

            if (pageFault) {
                continue;
            }

            if (rowState == NTable::ERowOp::Erase || rowState == NTable::ERowOp::Absent) {
                // in upsert if exists mode we must be sure that we insert only existing rows.
                continue;
            }
        }

        value.clear();
        size_t vi = 0;
        for (const auto& vt : valueCols) {
            if (valueCells.GetCells()[vi].Size() > NLimits::MaxWriteValueSize) {
                SetError(NKikimrTxDataShard::TError::BAD_ARGUMENT,
                         Sprintf("Row cell size of %" PRISZT " bytes is larger than the allowed threshold %" PRIu64,
                                 valueCells.GetBuffer().Size(), NLimits::MaxWriteValueSize));
                return true;
            }

            bool allowUpdate = true;
            if (readForTableShadow && rowState == NTable::ERowOp::Upsert && rowState.GetCellOp(vi) != NTable::ECellOp::Empty) {
                // We don't want to overwrite columns that already has some value
                allowUpdate = false;
            }

            if (allowUpdate) {
                value.emplace_back(NTable::TUpdateOp(vt.first, NTable::ECellOp::Set, TRawTypeValue(valueCells.GetCells()[vi].AsRef(), vt.second)));
            }
            ++vi;
        }

        if (readForTableShadow && rowState != NTable::ERowOp::Absent && value.empty()) {
            // We don't want to issue an Upsert when key already exists and there are no updates
            continue;
        }

        if (!writeToTableShadow) {
            // note, that for upsertIfExists mode we must break locks, because otherwise we can
            // produce inconsistency.
            if (BreakLocks) {
                if (breakWriteConflicts) {
                    if (!self->BreakWriteConflicts(txc.DB, fullTableId, keyCells.GetCells(), volatileDependencies)) {
                        pageFault = true;
                    }
                }

                if (!pageFault) {
                    self->SysLocksTable().BreakLocks(fullTableId, keyCells.GetCells());
                }
            }

            if (ChangeCollector) {
                Y_ABORT_UNLESS(CollectChanges);

                if (!volatileDependencies.empty()) {
                    if (!globalTxId) {
                        throw TNeedGlobalTxId();
                    }

                    if (!ChangeCollector->OnUpdateTx(fullTableId, writeTableId, NTable::ERowOp::Upsert, key, value, globalTxId)) {
                        pageFault = true;
                    }
                } else {
                    if (!ChangeCollector->OnUpdate(fullTableId, writeTableId, NTable::ERowOp::Upsert, key, value, writeVersion)) {
                        pageFault = true;
                    }
                }
            }

            if (pageFault) {
                continue;
            }
        }

        if (!volatileDependencies.empty()) {
            if (!globalTxId) {
                throw TNeedGlobalTxId();
            }
            txc.DB.UpdateTx(writeTableId, NTable::ERowOp::Upsert, key, value, globalTxId);
            self->GetConflictsCache().GetTableCache(writeTableId).AddUncommittedWrite(keyCells.GetCells(), globalTxId, txc.DB);
            if (!commitAdded) {
                // Make sure we see our own changes on further iterations
                userDb.AddCommitTxId(fullTableId, globalTxId, writeVersion);
                commitAdded = true;
            }
        } else {
            txc.DB.Update(writeTableId, NTable::ERowOp::Upsert, key, value, writeVersion);
            self->GetConflictsCache().GetTableCache(writeTableId).RemoveUncommittedWrites(keyCells.GetCells(), txc.DB);
        }
    }

    if (volatileReadDependencies && !userDb.GetVolatileReadDependencies().empty()) {
        *volatileReadDependencies = std::move(userDb.GetVolatileReadDependencies());
        txc.Reschedule();
        pageFault = true;
    }

    if (pageFault) {
        if (ChangeCollector) {
            ChangeCollector->OnRestart();
        }

        return false;
    }

    if (!volatileDependencies.empty()) {
        self->GetVolatileTxManager().PersistAddVolatileTx(
            globalTxId,
            writeVersion,
            /* commitTxIds */ { globalTxId },
            volatileDependencies,
            /* participants */ { },
            groupProvider.GetCurrentChangeGroup(),
            /* ordered */ false,
            /* arbiter */ false,
            txc);
        // Note: transaction is already committed, no additional waiting needed
    }

    self->IncCounter(COUNTER_UPLOAD_ROWS, record.GetRows().size());
    self->IncCounter(COUNTER_UPLOAD_ROWS_BYTES, bytes);

    tableInfo.Stats.UpdateTime = TAppData::TimeProvider->Now();
    return true;
}

template <typename TEvRequest, typename TEvResponse>
void TCommonUploadOps<TEvRequest, TEvResponse>::GetResult(TDataShard* self, TActorId& target, THolder<IEventBase>& event, ui64& cookie) {
    Y_ABORT_UNLESS(Result);

    if (Result->Record.GetStatus() == NKikimrTxDataShard::TError::OK) {
        self->IncCounter(COUNTER_BULK_UPSERT_SUCCESS);
    } else {
        self->IncCounter(COUNTER_BULK_UPSERT_ERROR);
    }

    target = Ev->Sender;
    event = std::move(Result);
    cookie = 0;
}

template <typename TEvRequest, typename TEvResponse>
const TEvRequest* TCommonUploadOps<TEvRequest, TEvResponse>::GetRequest() const {
    return Ev->Get();
}

template <typename TEvRequest, typename TEvResponse>
TEvResponse* TCommonUploadOps<TEvRequest, TEvResponse>::GetResult() {
    Y_ABORT_UNLESS(Result);
    return Result.Get();
}

template <typename TEvRequest, typename TEvResponse>
TVector<IDataShardChangeCollector::TChange> TCommonUploadOps<TEvRequest, TEvResponse>::GetCollectedChanges() const {
    if (!ChangeCollector) {
        return {};
    }

    auto changes = std::move(ChangeCollector->GetCollected());
    return changes;
}

template <typename TEvRequest, typename TEvResponse>
void TCommonUploadOps<TEvRequest, TEvResponse>::SetError(ui32 status, const TString& descr) {
    Result->Record.SetStatus(status);
    Result->Record.SetErrorDescription(descr);
}

template class TCommonUploadOps<TEvDataShard::TEvUploadRowsRequest, TEvDataShard::TEvUploadRowsResponse>;
template class TCommonUploadOps<TEvDataShard::TEvS3UploadRowsRequest, TEvDataShard::TEvS3UploadRowsResponse>;

}
