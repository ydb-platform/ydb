#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

class TCompactedWriteController : public NColumnShard::IWriteController {
private:
    class TBlobsConstructor : public IBlobConstructor {
        TCompactedWriteController& Owner;
        NOlap::TColumnEngineChanges& IndexChanges;

        ui64 CurrentPortion = 0;
        ui64 CurrentBlobIndex = 0;
        TPortionInfoWithBlobs::TBlobInfo* CurrentBlobInfo = nullptr;
    public:
        TBlobsConstructor(TCompactedWriteController& owner);
        const TString& GetBlob() const override;
        bool RegisterBlobId(const TUnifiedBlobId& blobId) override;
        EStatus BuildNext() override;
    };

    TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> WriteIndexEv;
    std::shared_ptr<TBlobsConstructor> BlobConstructor;
    TActorId DstActor;
protected:
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;
public:
    TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool blobGrouppingEnabled);
    ~TCompactedWriteController() {
        if (WriteIndexEv && WriteIndexEv->IndexChanges) {
            WriteIndexEv->IndexChanges->AbortEmergency();
        }
    }

    NOlap::IBlobConstructor::TPtr GetBlobConstructor() override {
        return BlobConstructor;
    }
};

}
