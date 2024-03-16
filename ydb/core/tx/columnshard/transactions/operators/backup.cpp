#include "backup.h"

namespace NKikimr::NColumnShard {

bool TBackupTransactionOperator::Parse(const TString& data) {
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
    ExportTask = std::make_shared<NOlap::NExport::TExportTask>(id.DetachResult(), selector.DetachResult(), storeInitializer.DetachResult());
    return true;
}

NKikimr::NColumnShard::TBackupTransactionOperator::TProposeResult TBackupTransactionOperator::Propose(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& /*txc*/, bool /*proposed*/) const {
    auto proposition = owner.GetExportsManager()->ProposeTask(ExportTask);
    if (!proposition) {
        return TProposeResult(NKikimrTxColumnShard::EResultStatus::ERROR,
            TStringBuilder() << "Invalid backup task TxId# " << GetTxId() << ": " << ExportTask->DebugString() << ": " << proposition.GetErrorMessage());
    }
    return TProposeResult();
}

bool TBackupTransactionOperator::Progress(TColumnShard& owner, const NOlap::TSnapshot& version, NTabletFlatExecutor::TTransactionContext& txc) {
    Y_UNUSED(version);
    AFL_VERIFY(ExportTask);
    owner.GetExportsManager()->ConfirmSessionOnExecute(ExportTask->GetIdentifier(), txc);
    return true;
}

bool TBackupTransactionOperator::Complete(TColumnShard& owner, const TActorContext& ctx) {
    AFL_VERIFY(ExportTask);
    owner.GetExportsManager()->ConfirmSessionOnComplete(ExportTask->GetIdentifier());
    auto result = std::make_unique<TEvColumnShard::TEvProposeTransactionResult>(
        owner.TabletID(), TxInfo.TxKind, GetTxId(), NKikimrTxColumnShard::SUCCESS);
    result->Record.SetStep(TxInfo.PlanStep);
    ctx.Send(TxInfo.Source, result.release(), 0, TxInfo.Cookie);
    return true;
}

bool TBackupTransactionOperator::Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) {
    owner.GetExportsManager()->RemoveSession(ExportTask->GetIdentifier(), txc);
    return true;
}

}
