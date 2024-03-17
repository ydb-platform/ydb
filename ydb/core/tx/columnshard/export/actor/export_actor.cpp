#include "export_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    CurrentData = data;
    CurrentDataBlob = Serializer->SerializeFull(CurrentData);
    if (data) {
        auto controller = std::make_shared<TWriteController>(SelfId(), std::vector<TString>({CurrentDataBlob}), BlobsOperator->StartWritingAction("EXPORT"), Cursor, ShardTabletId);
        Register(CreateWriteActor((ui64)ShardTabletId, controller, TInstant::Max()));
    }
    TOwnedCellVec lastKey = ev->Get()->LastKey;
    AFL_VERIFY(!Cursor.IsFinished());
    Cursor.InitNext(ev->Get()->LastKey, ev->Get()->Finished);
}

void TActor::HandleExecute(NEvents::TEvExportWritingFailed::TPtr& /*ev*/) {
    SwitchStage(EStage::WaitWriting, EStage::WaitWriting);
    auto controller = std::make_shared<TWriteController>(SelfId(), std::vector<TString>({CurrentDataBlob}), BlobsOperator->StartWritingAction("EXPORT"), Cursor, ShardTabletId);
    Register(CreateWriteActor((ui64)ShardTabletId, controller, TInstant::Max()));
}

}