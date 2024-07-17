#include "backup.h"
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/bg_tasks/manager/manager.h>
#include <ydb/core/formats/arrow/serializer/native.h>

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
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), selector.DetachResult(), storeInitializer.DetachResult(), serializer, GetTxId());
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

bool TBackupTransactionOperator::ProgressOnComplete(TColumnShard& /*owner*/, const TActorContext& /*ctx*/) {
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
