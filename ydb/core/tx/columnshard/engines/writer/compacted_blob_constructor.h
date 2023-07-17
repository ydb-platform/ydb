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
        const NOlap::TColumnEngineChanges& IndexChanges;
        const std::vector<TString>& Blobs;

        const bool BlobGrouppingEnabled;
        const bool CacheData;
        const bool EvictionFlag;

        TString AccumulatedBlob;
        std::vector<std::pair<size_t, TString>> RecordsInBlob;

        ui64 CurrentPortion = 0;
        ui64 LastPortion = 0;

        ui64 CurrentBlob = 0;
        ui64 CurrentPortionRecord = 0;

        TVector<NOlap::TPortionInfo> PortionUpdates;

    public:
        TBlobsConstructor(TCompactedWriteController& owner, bool blobGrouppingEnabled);
        const TString& GetBlob() const override;
        bool RegisterBlobId(const TUnifiedBlobId& blobId) override;
        EStatus BuildNext() override;

        const  TVector<NOlap::TPortionInfo>& GetPortionUpdates() const {
            return PortionUpdates;
        }

        bool IsEviction() const {
            return EvictionFlag;
        }

    private:
        const NOlap::TPortionInfo& GetPortionInfo(const ui64 index) const;
    };

    TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> WriteIndexEv;
    std::shared_ptr<TBlobsConstructor> BlobConstructor;
    TActorId DstActor;
public:
    TCompactedWriteController(const TActorId& dstActor, TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeEv, bool blobGrouppingEnabled);

    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;

    NOlap::IBlobConstructor::TPtr GetBlobConstructor() override {
        return BlobConstructor;
    }
};

}
