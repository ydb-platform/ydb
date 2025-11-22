#include "backup.h"
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NColumnShard {

bool TBackupTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrTxColumnShard::TBackupTxBody txBody;
    if (!txBody.ParseFromString(data)) {
        return false;
    }
    if (!txBody.HasBackupTask()) {
        return false;
    }
    TConclusion<NOlap::NExport::TIdentifier> id = NOlap::NExport::TIdentifier::BuildFromProto(txBody);
    if (!id) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_id")("problem", id.GetErrorMessage());
        return false;
    }
    auto schema = owner.TablesManager.GetPrimaryIndex()->GetVersionedIndex().GetSchemaVerified(NKikimr::NOlap::TSnapshot{txBody.GetBackupTask().GetSnapshotStep(), txBody.GetBackupTask().GetSnapshotTxId()});
    auto columns = schema->GetIndexInfo().GetColumns();
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), columns, txBody.GetBackupTask(), GetTxId());
    NOlap::NBackground::TTask task(::ToString(ExportTask->GetIdentifier().GetPathId()), std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ExportTask);
    if (!owner.GetBackgroundSessionsManager()->HasTask(task)) {
        TxAddTask = owner.GetBackgroundSessionsManager()->TxAddTask(task);
        if (!TxAddTask) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_task");
            return false;
        }
    } else {
        TaskExists = true;
    }
    return true;
}

TBackupTransactionOperator::TProposeResult TBackupTransactionOperator::DoStartProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TaskExists) {
        AFL_VERIFY(!!TxAddTask);
        AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    }
    return TProposeResult();
}

void TBackupTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!TaskExists) {
        AFL_VERIFY(!!TxAddTask);
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }
}

bool TBackupTransactionOperator::ProgressOnExecute(
    TColumnShard& /*owner*/, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
    return true;
}

bool TBackupTransactionOperator::ProgressOnComplete(TColumnShard& owner, const TActorContext& ctx) {
    for (TActorId subscriber : NotifySubscribers) {
        auto event = MakeHolder<TEvColumnShard::TEvNotifyTxCompletionResult>(owner.TabletID(), GetTxId());
        ctx.Send(subscriber, event.Release(), 0, 0);
    }
    return true;
}

bool TBackupTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TxAbort) {
        auto control = ExportTask->BuildAbortControl();
        TxAbort = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }
    return TxAbort->Execute(txc, NActors::TActivationContext::AsActorContext());
}

}
