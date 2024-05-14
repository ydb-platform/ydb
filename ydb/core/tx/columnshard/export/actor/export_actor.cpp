#include "export_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    if (data) {
        CurrentData = NArrow::ToBatch(data, true);
        CurrentDataBlob = ExportSession->GetTask().GetSerializer()->SerializeFull(CurrentData);
        if (data) {
            auto controller = std::make_shared<TWriteController>(SelfId(), std::vector<TString>({CurrentDataBlob}), 
                BlobsOperator->StartWritingAction(NBlobOperations::EConsumer::EXPORT), ExportSession->GetCursor(), TabletId, ExportSession->GetTask().GetSelector()->GetPathId());
            Register(CreateWriteActor((ui64)TabletId, controller, TInstant::Max()));
        }
    } else {
        CurrentData = nullptr;
        CurrentDataBlob = "";
        TBase::Send(SelfId(), new NEvents::TEvExportWritingFinished);
    }
    TOwnedCellVec lastKey = ev->Get()->LastKey;
    AFL_VERIFY(!ExportSession->GetCursor().IsFinished());
    ExportSession->MutableCursor().InitNext(ev->Get()->LastKey, ev->Get()->Finished);
}

void TActor::HandleExecute(NEvents::TEvExportWritingFailed::TPtr& /*ev*/) {
    SwitchStage(EStage::WaitWriting, EStage::WaitWriting);
    auto controller = std::make_shared<TWriteController>(SelfId(), std::vector<TString>({CurrentDataBlob}), 
        BlobsOperator->StartWritingAction(NBlobOperations::EConsumer::EXPORT),
        ExportSession->GetCursor(), TabletId, ExportSession->GetTask().GetSelector()->GetPathId());
    Register(CreateWriteActor((ui64)TabletId, controller, TInstant::Max()));
}

}