#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/formats/arrow/size_calcer.h>


namespace NKikimr::NOlap {

class TIndexedWriteController : public NColumnShard::IWriteController, public NColumnShard::TMonitoringObjectsCounter<TIndexedWriteController, true> {
private:
    std::vector<NArrow::TSerializedBatch> BlobsSplitted;
    NEvWrite::TWriteData WriteData;
    TVector<NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData> BlobData;
    TActorId DstActor;
    std::shared_ptr<IBlobsWritingAction> Action;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;
    virtual void DoOnStartSending() override;

public:
    TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData, const std::shared_ptr<IBlobsWritingAction>& action, std::vector<NArrow::TSerializedBatch>&& blobsSplitted);

};

}
