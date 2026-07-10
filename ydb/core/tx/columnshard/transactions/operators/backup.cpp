#include "backup.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/transactions/transactions/tx_finish_async.h>

namespace NKikimr::NColumnShard {

bool TBackupTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrTxColumnShard::TBackupTxBody txBody;
    if (!txBody.ParseFromString(data)) {
        return false;
    }
    if (!txBody.HasBackupTask()) {
        return false;
    }

    if (const auto* completedTx = owner.LastCompletedBackupTransactionsByTxId.FindPtr(GetTxId())) {
<<<<<<< HEAD
        if (completedTx->GetOpResult().GetSuccess()) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "backup_already_completed")("tx_id", GetTxId())(
                "success", completedTx->GetOpResult().GetSuccess())("explain", completedTx->GetOpResult().GetExplain());
            AlreadyCompleted = true;
        }
=======
        YDB_LOG_INFO("",
            {"event", "backup_already_completed"},
            {"txId", GetTxId()},
            {"success", completedTx->GetOpResult().GetSuccess()},
            {"explain", completedTx->GetOpResult().GetExplain()});
        AlreadyCompleted = true;
>>>>>>> abb000a3607 (simplify tx (#45970))
        return true;
    }

    TConclusion<NOlap::NExport::TIdentifier> id = NOlap::NExport::TIdentifier::BuildFromProto(txBody);
    if (!id) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_id")("problem", id.GetErrorMessage());
        return false;
    }
    auto schema = owner.TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaVerified(
        NKikimr::NOlap::TSnapshot{ txBody.GetBackupTask().GetSnapshotStep(), txBody.GetBackupTask().GetSnapshotTxId() });
    const auto& indexInfo = schema->GetIndexInfo();
    const auto& columnsMap = indexInfo.GetColumns();
    std::vector<NOlap::TNameTypeInfo> columns;
    columns.reserve(columnsMap.size());
    for (const ui32 columnId : indexInfo.GetColumnIds(false)) {
        auto it = columnsMap.find(columnId);
        AFL_VERIFY(it != columnsMap.end())("column_id", columnId);
        columns.emplace_back(it->second);
    }
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), columns, txBody.GetBackupTask(), GetTxId());
    return true;
}

TBackupTransactionOperator::TProposeResult TBackupTransactionOperator::DoStartProposeOnExecute(
    TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (AlreadyCompleted) {
        return TProposeResult();
    }
    NOlap::NBackground::TTask task(::ToString(ExportTask->GetIdentifier().GetSchemeShardLocalPathId().GetRawValue()),
        std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ExportTask);
    if (!owner.GetBackgroundSessionsManager()->HasTask(task)) {
        TxAddTask = owner.GetBackgroundSessionsManager()->TxAddTask(task);
        if (!TxAddTask) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_task");
            return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR, "Cannot add backup task");
        }
        AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    } else {
        TaskExists = true;
    }
    return TProposeResult();
}

void TBackupTransactionOperator::DoStartProposeOnComplete(TColumnShard& owner, const TActorContext& ctx) {
    if (!TaskExists && TxAddTask) {
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }

    if (AlreadyCompleted) {
        owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()), ctx);
        return;
    }
    if (!ExportTask) {
        return;
    }
    const auto pathId = ExportTask->GetIdentifier().GetSchemeShardLocalPathId();
    if (!owner.GetBackgroundSessionsManager()->IsSessionComplete(ExportTask->GetClassName(), ::ToString(pathId.GetRawValue()))) {
        return;
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "backup_session_complete_finish_async_propose")("tx_id", GetTxId());
    owner.Execute(new TTxFinishAsyncTransaction(owner, GetTxId()), ctx);
}

bool TBackupTransactionOperator::ProgressOnExecute(
    TColumnShard& owner, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (AlreadyCompleted) {
        return true;
    }
    AFL_VERIFY(!TxRemove);
    const auto schemeShardLocalPathId = ExportTask->GetIdentifier().GetSchemeShardLocalPathId();
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(ExportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));

    NKikimrTxColumnShard::TCompletedBackupTransaction backupTx;
    backupTx.SetTxId(GetTxId());
    auto& opResult = *backupTx.MutableOpResult();
    opResult.SetSuccess(status.Success);
    opResult.SetExplain(status.ErrorMessage);

    TxRemove = owner.GetBackgroundSessionsManager()->TxRemove(ExportTask->GetClassName(), ::ToString(schemeShardLocalPathId.GetRawValue()));
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

bool TBackupTransactionOperator::ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) {
    if (AlreadyCompleted) {
        return true;
    }
    auto status = owner.GetBackgroundSessionsManager()->GetStatus(
        ExportTask->GetClassName(), ::ToString(ExportTask->GetIdentifier().GetSchemeShardLocalPathId().GetRawValue()));
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

bool TBackupTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!ExportTask) {
        return true;
    }
    if (!TxAbort) {
        auto control = ExportTask->BuildAbortControl();
        TxAbort = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }

    const auto schemeShardLocalPathId = ExportTask->GetIdentifier().GetSchemeShardLocalPathId();
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

bool TBackupTransactionOperator::CompleteOnAbort(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (TxAbort) {
        TxAbort->Complete(ctx);
    }
    return true;
}

void TBackupTransactionOperator::RegisterSubscriber(const TActorId& actorId) {
    NotifySubscribers.insert(actorId);
}

TString TBackupTransactionOperator::DoDebugString() const {
    return "BACKUP";
}

bool TBackupTransactionOperator::DoIsAsync() const {
    return true;
}

TString TBackupTransactionOperator::DoGetOpType() const {
    return "Backup";
}

void TBackupTransactionOperator::DoFinishProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
}

void TBackupTransactionOperator::DoFinishProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
}

}   // namespace NKikimr::NColumnShard
