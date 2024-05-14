#include "backup.h"
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/formats/arrow/serializer/native.h>

namespace NKikimr::NColumnShard {

bool TBackupTransactionOperator::Parse(TColumnShard& owner, const TString& data) {
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
    TConclusion<NOlap::NExport::TSelectorContainer> selector = NOlap::NExport::TSelectorContainer::BuildFromProto(txBody);
    if (!selector) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_selector")("problem", selector.GetErrorMessage());
        return false;
    }
    TConclusion<NOlap::NExport::TStorageInitializerContainer> storeInitializer = NOlap::NExport::TStorageInitializerContainer::BuildFromProto(txBody);
    if (!storeInitializer) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_parse_selector")("problem", storeInitializer.GetErrorMessage());
        return false;
    }
    NArrow::NSerialization::TSerializerContainer serializer(std::make_shared<NArrow::NSerialization::TNativeSerializer>());
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), selector.DetachResult(), storeInitializer.DetachResult(), serializer);
    NOlap::NBackground::TTask task(::ToString(ExportTask->GetIdentifier().GetPathId()), std::make_shared<NOlap::NBackground::TFakeStatusChannel>(), ExportTask);
    TxAddTask = owner.GetBackgroundSessionsManager()->TxAddTask(task);
    if (!TxAddTask) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "cannot_add_task");
        return false;
    }
    return true;
}

TBackupTransactionOperator::TProposeResult TBackupTransactionOperator::ExecuteOnPropose(TColumnShard& /*owner*/, NTabletFlatExecutor::TTransactionContext& txc) const {
    AFL_VERIFY(!!TxAddTask);
    AFL_VERIFY(TxAddTask->Execute(txc, NActors::TActivationContext::AsActorContext()));
    return TProposeResult();
}

bool TBackupTransactionOperator::CompleteOnPropose(TColumnShard& /*owner*/, const TActorContext& ctx) const {
    AFL_VERIFY(!!TxAddTask);
    TxAddTask->Complete(ctx);
    return true;
}

bool TBackupTransactionOperator::ExecuteOnProgress(TColumnShard& owner, const NOlap::TSnapshot& /*version*/, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(ExportTask);
    if (!TxConfirm) {
        auto control = ExportTask->BuildConfirmControl();
        TxConfirm = owner.GetBackgroundSessionsManager()->TxApplyControl(control);
    }
    return TxConfirm->Execute(txc, NActors::TActivationContext::AsActorContext());
}

bool TBackupTransactionOperator::CompleteOnProgress(TColumnShard& owner, const TActorContext& ctx) {
    AFL_VERIFY(ExportTask);
    AFL_VERIFY(!!TxConfirm);
    TxConfirm->Complete(ctx);

    auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
        owner.TabletID(), TxInfo.TxKind, GetTxId(), NKikimrTxColumnShard::SUCCESS);
    result->Record.SetStep(TxInfo.PlanStep);
    ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
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
