#include "restore.h"

#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NColumnShard {

bool TRestoreTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrTxColumnShard::TRestoreTxBody txBody;
    if (!txBody.ParseFromString(data)) {
        return false;
    }
    if (!txBody.HasRestoreTask()) {
        return false;
    }

    if (const auto* completedTx = owner.LastCompletedBackupTransactionsByTxId.FindPtr(GetTxId())) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "restore_already_completed")("tx_id", GetTxId())(
            "success", completedTx->GetOpResult().GetSuccess())("explain", completedTx->GetOpResult().GetExplain());
        AlreadyCompleted = true;
        return true;
    }

    auto schema = owner.TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetLastSchema();
    const auto& columns = schema->GetIndexInfo().GetColumns();
    const auto schemeShardLocalPathId = TSchemeShardLocalPathId::FromRawValue(txBody.GetRestoreTask().GetTableId());
    ImportTask =
        std::make_shared<NOlap::NImport::TImportTask>(schemeShardLocalPathId, columns, txBody.GetRestoreTask(), schema->GetVersion(), GetTxId());
    return true;
}

TRestoreTransactionOperator::TProposeResult TRestoreTransactionOperator::DoStartProposeOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (AlreadyCompleted) {
        return TProposeResult();
    }
    NOlap::NBackground::TTask task(::ToString(ImportTask->GetSchemeShardLocalPathId().GetRawValue()),
        std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ImportTask);
    if (!owner.GetBackgroundSessionsManager()->HasTask(task)) {
        TxAddTask = owner.GetBackgroundSessionsManager()->TxAddTask(task);
        if (!TxAddTask) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_task");
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "Cannot add restore task");
        }
        AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    } else {
        TaskExists = true;
    }
    return TProposeResult();
}

void TRestoreTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!TaskExists && TxAddTask) {
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }
}

bool TRestoreTransactionOperator::ProgressOnExecute(
    TColumnShard& owner, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (AlreadyCompleted) {
        return true;
    }
    AFL_VERIFY(!TxRemove);
    const auto schemeShardLocalPathId = ImportTask->GetSchemeShardLocalPathId();
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(ImportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));

    NKikimrTxColumnShard::TCompletedBackupTransaction backupTx;
    backupTx.SetTxId(GetTxId());
    auto& opResult = *backupTx.MutableOpResult();
    opResult.SetSuccess(status.Success);
    opResult.SetExplain(status.ErrorMessage);

    TxRemove = owner.GetBackgroundSessionsManager()->TxRemove(ImportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));
    NIceDb::TNiceDb db(txc.DB);

    const auto tableId = owner.TablesManager.ResolveInternalPathIdVerified(schemeShardLocalPathId, false);
    if (const auto* previousBackupTx = owner.LastCompletedBackupTransactions.FindPtr(schemeShardLocalPathId)) {
        owner.LastCompletedBackupTransactionsByTxId.erase(previousBackupTx->GetTxId());
    }
    const TString serializedBackupTx = backupTx.SerializeAsString();
    owner.LastCompletedBackupTransactions[schemeShardLocalPathId] = backupTx;
    owner.LastCompletedBackupTransactionsByTxId[backupTx.GetTxId()] = backupTx;
    owner.TablesManager.SetLastCompletedBackupTransaction(schemeShardLocalPathId, serializedBackupTx);

    db.Table<Schema::TableInfoV1>()
        .Key(tableId.GetRawValue(), schemeShardLocalPathId.GetRawValue())
        .Update(NIceDb::TUpdate<Schema::TableInfoV1::LastCompletedBackupTransaction>(serializedBackupTx));

    return TxRemove->Execute(txc, NActors::TActivationContext::AsActorContext());
}

bool TRestoreTransactionOperator::ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) {
    if (AlreadyCompleted) {
        return true;
    }
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(
        ImportTask->GetClassName(), ::ToString(ImportTask->GetSchemeShardLocalPathId().GetRawValue()));
    for (TActorId subscriber : NotifySubscribers) {
        auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
        auto& opResult = *event->Record.MutableOpResult();
        opResult.SetSuccess(status.Success);
        opResult.SetExplain(status.ErrorMessage);
        ctx.Send(subscriber, event.Release(), 0, 0);
    }
    AFL_VERIFY(!!TxRemove);
    TxRemove->Complete(NActors::TActivationContext::AsActorContext());
    return true;
}

bool TRestoreTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!ImportTask) {
        return true;
    }
    if (!TxAbort) {
        auto control = ImportTask->BuildAbortControl();
        TxAbort = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }

    const auto schemeShardLocalPathId = ImportTask->GetSchemeShardLocalPathId();
    NKikimrTxColumnShard::TCompletedBackupTransaction backupTx;
    backupTx.SetTxId(GetTxId());
    auto& opResult = *backupTx.MutableOpResult();
    opResult.SetSuccess(false);
    opResult.SetExplain("Cancelled");

    NIceDb::TNiceDb db(txc.DB);
    const auto tableId = owner.TablesManager.ResolveInternalPathIdVerified(schemeShardLocalPathId, false);
    if (const auto* previousBackupTx = owner.LastCompletedBackupTransactions.FindPtr(schemeShardLocalPathId)) {
        owner.LastCompletedBackupTransactionsByTxId.erase(previousBackupTx->GetTxId());
    }
    const TString serializedBackupTx = backupTx.SerializeAsString();
    owner.LastCompletedBackupTransactions[schemeShardLocalPathId] = backupTx;
    owner.LastCompletedBackupTransactionsByTxId[backupTx.GetTxId()] = backupTx;
    owner.TablesManager.SetLastCompletedBackupTransaction(schemeShardLocalPathId, serializedBackupTx);

    db.Table<Schema::TableInfoV1>()
        .Key(tableId.GetRawValue(), schemeShardLocalPathId.GetRawValue())
        .Update(NIceDb::TUpdate<Schema::TableInfoV1::LastCompletedBackupTransaction>(serializedBackupTx));

    if (!TxAbort) {
        return true;
    }
    return TxAbort->Execute(txc, NActors::TActivationContext::AsActorContext());
}

bool TRestoreTransactionOperator::CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (TxAbort) {
        TxAbort->Complete(ctx);
    }
    return true;
}

void TRestoreTransactionOperator::RegisterSubscriber(const TActorId& actorId) {
    NotifySubscribers.insert(actorId);
}

TString TRestoreTransactionOperator::DoDebugString() const {
    return "RESTORE";
}

bool TRestoreTransactionOperator::DoIsAsync() const {
    return !AlreadyCompleted;
}

bool TRestoreTransactionOperator::DoIsProposeReplyReady(TColumnShard& owner) const {
    if (AlreadyCompleted || !ImportTask) {
        return true;
    }
    const auto schemeShardLocalPathId = ImportTask->GetSchemeShardLocalPathId();
    return owner.GetBackgroundSessionsManager()->IsSessionComplete(ImportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));
}

TString TRestoreTransactionOperator::DoGetOpType() const {
    return "Restore";
}

void TRestoreTransactionOperator::DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
}

void TRestoreTransactionOperator::DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
}

}   // namespace NKikimr::NColumnShard
