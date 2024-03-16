#include "export_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    CurrentData = data;
    CurrentDataBlobs = ExternalControl->BatchToBlobs(CurrentData);
    if (data) {
        Register(CreateWriteActor((ui64)ShardTabletId, std::make_shared<TWriteController>(SelfId(), CurrentDataBlobs, BlobsOperator->StartWritingAction("EXPORT")), TInstant::Max()));
        LastKey = ev->Get()->LastKey;
    }
    AFL_VERIFY(!LastMessageReceived);
    LastMessageReceived = ev->Get()->Finished;
}

void TActor::HandleExecute(NEvents::TEvExportWritingFailed::TPtr& /*ev*/) {
    Register(CreateWriteActor((ui64)ShardTabletId, std::make_shared<TWriteController>(SelfId(), CurrentDataBlobs, BlobsOperator->StartWritingAction("EXPORT")), TInstant::Max()));
}

}