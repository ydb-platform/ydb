#include "restore.h"
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/formats/arrow/serializer/native.h>

namespace NKikimr::NColumnShard {

bool TRestoreTransactionOperator::DoParse(TColumnShard& owner, const TString& data) {
    NKikimrTxColumnShard::TRestoreTxBody txBody;
    if (!txBody.ParseFromString(data)) {
        return false;
    }
    if (!txBody.HasRestoreTask()) {
        return false;
    }
    ImportTask = std::make_shared<NOlap::NImport::TImportTask>(TInternalPathId::FromRawValue(txBody.GetRestoreTask().GetTableId()), GetTxId());
    NOlap::NBackground::TTask task(::ToString(txBody.GetRestoreTask().GetTableId()), std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ImportTask);
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

TRestoreTransactionOperator::TProposeResult TRestoreTransactionOperator::DoStartProposeOnExecute(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TaskExists) {
        AFL_VERIFY(!!TxAddTask);
        AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    }
    return TProposeResult();
}

void TRestoreTransactionOperator::DoStartProposeOnComplete(TColumnShard& /*owner*/, const TActorContext& ctx) {
    if (!TaskExists) {
        AFL_VERIFY(!!TxAddTask);
        TxAddTask->Complete(ctx);
        TxAddTask.reset();
    }
}

bool TRestoreTransactionOperator::ProgressOnExecute(
    TColumnShard& /*owner*/, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& /*txc*/) {
    return true;
}

bool TRestoreTransactionOperator::ProgressOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
    return true;
}

bool TRestoreTransactionOperator::ExecuteOnAbort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    if (!TxAbort) {
        auto control = ImportTask->BuildAbortControl();
        TxAbort = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }
    return TxAbort->Execute(txc, NActors::TActivationContext::AsActorContext());
}

TString TRestoreTransactionOperator::DoDebugString() const {
    return "RESTORE";
}

bool TRestoreTransactionOperator::CompleteOnAbort(TColumnShard & /*owner*/, const TActorContext & /*ctx*/) {
    return true;
}

bool TRestoreTransactionOperator::DoIsAsync() const {
    return true;
}

TString TRestoreTransactionOperator::DoGetOpType() const {
    return "Restore";
}

void TRestoreTransactionOperator::DoFinishProposeOnComplete(TColumnShard & /*owner*/, const TActorContext & /*ctx*/) {
}

void TRestoreTransactionOperator::DoFinishProposeOnExecute(TColumnShard & /*owner*/, NTabletFlatExecutor::TTransactionContext & /*txc*/) {
}

} // namespace NKikimr::NColumnShard
