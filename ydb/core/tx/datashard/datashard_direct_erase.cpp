#include "change_collector.h"
#include "datashard_direct_erase.h"
#include "datashard_user_db.h"
#include "erase_rows_condition.h"

#include <ydb/core/base/appdata.h>

#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard {

TDirectTxErase::TDirectTxErase(TEvDataShard::TEvEraseRowsRequest::TPtr& ev)
    : Ev(ev)
{
}

TDirectTxErase::EStatus TDirectTxErase::CheckedExecute(
        TDataShard* self, const TExecuteParams& params,
        const NKikimrTxDataShard::TEvEraseRowsRequest& request,
        NKikimrTxDataShard::TEvEraseRowsResponse::EStatus& status, TString& error)
{
    const ui64 tableId = request.GetTableId();
    const TTableId fullTableId(self->GetPathOwnerId(), tableId);
    const ui64 localTableId = self->GetLocalTableId(fullTableId);
    if (localTableId == 0) {
        status = NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR;
        error = TStringBuilder() << "Unknown table id: " << tableId;
        return EStatus::Error;
    }

    const TUserTable& tableInfo = *self->GetUserTables().at(tableId);
    Y_ABORT_UNLESS(tableInfo.LocalTid == localTableId);

    if (request.GetSchemaVersion() && tableInfo.GetTableSchemaVersion()
        && request.GetSchemaVersion() != tableInfo.GetTableSchemaVersion()) {

        status = NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR;
        error = TStringBuilder() << "Schema version mismatch"
            << ": got " << request.GetSchemaVersion()
            << ", expected " << tableInfo.GetTableSchemaVersion();
        return EStatus::Error;
    }

    if (request.KeyColumnIdsSize() != tableInfo.KeyColumnIds.size()) {
        status = NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR;
        error = TStringBuilder() << "Key column count mismatch"
            << ": got " << request.KeyColumnIdsSize()
            << ", expected " << tableInfo.KeyColumnIds.size();
        return EStatus::Error;
    }

    for (size_t i = 0; i < tableInfo.KeyColumnIds.size(); ++i) {
        if (request.GetKeyColumnIds(i) != tableInfo.KeyColumnIds[i]) {
            status = NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR;
            error = TStringBuilder() << "Key column schema mismatch at position: " << i;
            return EStatus::Error;
        }
    }

    std::optional<NMiniKQL::TEngineHostCounters> engineHostCounters;
    std::optional<TDataShardUserDb> userDb;
    std::optional<TDataShardChangeGroupProvider> groupProvider;

    THolder<IEraseRowsCondition> condition;
    if (params) {
        condition.Reset(CreateEraseRowsCondition(request));
        if (condition) {
            condition->Prepare(params.Txc->DB.GetRowScheme(localTableId), 0);
        }

        engineHostCounters.emplace();
        userDb.emplace(*self, params.Txc->DB, params.GlobalTxId, params.ReadVersion, params.WriteVersion, *engineHostCounters, TAppData::TimeProvider->Now());
        groupProvider.emplace(*self, params.Txc->DB);
        params.Tx->ChangeCollector.Reset(CreateChangeCollector(*self, *userDb, *groupProvider, params.Txc->DB, tableInfo));
    }

    const bool breakWriteConflicts = (
        self->SysLocksTable().HasWriteLocks(fullTableId) ||
        self->GetVolatileTxManager().GetTxMap());

    absl::flat_hash_set<ui64> volatileDependencies;

    bool pageFault = false;
    bool commitAdded = false;
    for (const auto& serializedKey : request.GetKeyColumns()) {
        TSerializedCellVec keyCells;
        if (!TSerializedCellVec::TryParse(serializedKey, keyCells)) {
            status = NKikimrTxDataShard::TEvEraseRowsResponse::BAD_REQUEST;
            error = "Cannot parse key";
            return EStatus::Error;
        }

        if (keyCells.GetCells().size() != tableInfo.KeyColumnTypes.size()) {
            status = NKikimrTxDataShard::TEvEraseRowsResponse::SCHEME_ERROR;
            error = "Cell count doesn't match row scheme";
            return EStatus::Error;
        }

        ui64 keyBytes = 0;
        TVector<TRawTypeValue> key;
        for (size_t ki : xrange(tableInfo.KeyColumnTypes.size())) {
            const auto& kt = tableInfo.KeyColumnTypes[ki];
            const TCell& cell = keyCells.GetCells()[ki];

            if (kt.GetTypeId() == NScheme::NTypeIds::Uint8 && !cell.IsNull() && cell.AsValue<ui8>() > 127) {
                status = NKikimrTxDataShard::TEvEraseRowsResponse::BAD_REQUEST;
                error = "Keys with Uint8 column values >127 are currently prohibited";
                return EStatus::Error;
            }

            keyBytes += cell.Size();
            key.emplace_back(TRawTypeValue(cell.AsRef(), kt));
        }

        if (keyBytes > NLimits::MaxWriteKeySize) {
            status = NKikimrTxDataShard::TEvEraseRowsResponse::BAD_REQUEST;
            error = TStringBuilder() << "Key is too big"
                << ": actual " << keyBytes << " bytes"
                << ", limit " << NLimits::MaxWriteKeySize << " bytes";
            return EStatus::Error;
        }

        if (!params) {
            continue;
        }

        if (condition) {
            NTable::TRowState row;
            const auto ready = userDb->SelectRow(fullTableId, key, condition->Tags(), row);

            switch (ready) {
            case NTable::EReady::Page:
                pageFault = true;
                break;
            case NTable::EReady::Gone:
                continue;
            case NTable::EReady::Data:
                if (!condition->Check(row)) {
                    continue;
                }
                break;
            }
        }

        if (breakWriteConflicts) {
            if (!self->BreakWriteConflicts(params.Txc->DB, fullTableId, keyCells.GetCells(), volatileDependencies)) {
                pageFault = true;
            }
        }

        if (auto collector = params.GetChangeCollector()) {
            if (!volatileDependencies.empty()) {
                if (!params.GlobalTxId) {
                    throw TNeedGlobalTxId();
                }

                if (!collector->OnUpdateTx(fullTableId, localTableId, NTable::ERowOp::Erase, key, {}, params.GlobalTxId)) {
                    pageFault = true;
                }
            } else {
                if (!collector->OnUpdate(fullTableId, localTableId, NTable::ERowOp::Erase, key, {}, params.WriteVersion)) {
                    pageFault = true;
                }
            }
        }

        if (pageFault) {
            continue;
        }

        self->SysLocksTable().BreakLocks(fullTableId, keyCells.GetCells());

        if (!volatileDependencies.empty()) {
            if (!params.GlobalTxId) {
                throw TNeedGlobalTxId();
            }
            params.Txc->DB.UpdateTx(localTableId, NTable::ERowOp::Erase, key, {}, params.GlobalTxId);
            self->GetConflictsCache().GetTableCache(localTableId).AddUncommittedWrite(keyCells.GetCells(), params.GlobalTxId, params.Txc->DB);
            if (!commitAdded && userDb) {
                // Make sure we see our own changes on further iterations
                userDb->AddCommitTxId(fullTableId, params.GlobalTxId, params.WriteVersion);
                commitAdded = true;
            }
        } else {
            params.Txc->DB.Update(localTableId, NTable::ERowOp::Erase, key, {}, params.WriteVersion);
            self->GetConflictsCache().GetTableCache(localTableId).RemoveUncommittedWrites(keyCells.GetCells(), params.Txc->DB);
        }
    }

    if (params.VolatileReadDependencies && userDb && !userDb->GetVolatileReadDependencies().empty()) {
        *params.VolatileReadDependencies = std::move(userDb->GetVolatileReadDependencies());
        params.Txc->Reschedule();
        pageFault = true;
    }

    if (pageFault) {
        if (auto collector = params.GetChangeCollector()) {
            collector->OnRestart();
        }

        return EStatus::PageFault;
    }

    if (!volatileDependencies.empty()) {
        self->GetVolatileTxManager().PersistAddVolatileTx(
            params.GlobalTxId,
            params.WriteVersion,
            /* commitTxIds */ { params.GlobalTxId },
            volatileDependencies,
            /* participants */ { },
            groupProvider ? groupProvider->GetCurrentChangeGroup() : std::nullopt,
            /* ordered */ false,
            /* arbiter */ false,
            *params.Txc);
        // Note: transaction is already committed, no additional waiting needed
    }

    status = NKikimrTxDataShard::TEvEraseRowsResponse::OK;
    return EStatus::Success;
}

bool TDirectTxErase::CheckRequest(TDataShard* self, const NKikimrTxDataShard::TEvEraseRowsRequest& request,
        NKikimrTxDataShard::TEvEraseRowsResponse::EStatus& status, TString& error)
{
    const auto result = CheckedExecute(self, TExecuteParams::ForCheck(), request, status, error);
    switch (result) {
    case EStatus::Success:
        return true;
    case EStatus::Error:
        return false;
    case EStatus::PageFault:
        Y_ABORT("Unexpected");
    }
}

bool TDirectTxErase::Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& readVersion, const TRowVersion& writeVersion,
        ui64 globalTxId, absl::flat_hash_set<ui64>& volatileReadDependencies)
{
    const auto& record = Ev->Get()->Record;

    Result = MakeHolder<TEvDataShard::TEvEraseRowsResponse>();
    Result->Record.SetTabletID(self->TabletID());

    const auto params = TExecuteParams::ForExecute(this, &txc, readVersion, writeVersion,
        globalTxId, &volatileReadDependencies);
    NKikimrTxDataShard::TEvEraseRowsResponse::EStatus status;
    TString error;

    const auto result = CheckedExecute(self, params, record, status, error);
    switch (result) {
    case EStatus::Success:
    case EStatus::Error:
        break;
    case EStatus::PageFault:
        return false;
    }

    Result->Record.SetStatus(status);
    Result->Record.SetErrorDescription(error);

    self->IncCounter(COUNTER_ERASE_ROWS, record.GetKeyColumns().size());
    if (self->GetUserTables().contains(record.GetTableId())) {
        self->GetUserTables().at(record.GetTableId())->Stats.UpdateTime = TAppData::TimeProvider->Now();
    }

    return true;
}

TDirectTxResult TDirectTxErase::GetResult(TDataShard* self) {
    Y_ABORT_UNLESS(Result);

    if (Result->Record.GetStatus() == NKikimrTxDataShard::TEvEraseRowsResponse::OK) {
        self->IncCounter(COUNTER_ERASE_ROWS_SUCCESS);
    } else {
        self->IncCounter(COUNTER_ERASE_ROWS_ERROR);
    }

    TDirectTxResult res;
    res.Target = Ev->Sender;
    res.Event = std::move(Result);
    res.Cookie = 0;
    return res;
}

TVector<IDataShardChangeCollector::TChange> TDirectTxErase::GetCollectedChanges() const {
    if (!ChangeCollector) {
        return {};
    }

    auto changes = std::move(ChangeCollector->GetCollected());
    return changes;
}

} // NDataShard
} // NKikimr
