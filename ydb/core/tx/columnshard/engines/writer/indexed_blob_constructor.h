#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/ev_write/write_data.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/formats/arrow/size_calcer.h>


namespace NKikimr::NOlap {

class TIndexedWriteController : public NColumnShard::IWriteController {
private:
    class TBlobConstructor : public IBlobConstructor {
        TIndexedWriteController& Owner;
        std::vector<NArrow::TSerializedBatch> BlobsSplitted;

        ui64 CurrentIndex = 0;
    public:
        TBlobConstructor(TIndexedWriteController& owner);

        const TString& GetBlob() const override;
        EStatus BuildNext() override;
        bool RegisterBlobId(const TUnifiedBlobId& blobId) override;
        bool Init();
    };

    NEvWrite::TWriteData WriteData;
    std::shared_ptr<TBlobConstructor> BlobConstructor;
    TVector<NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData> BlobData;
    TActorId DstActor;

public:
    TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData);

    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;

    NOlap::IBlobConstructor::TPtr GetBlobConstructor() override;
};

}
