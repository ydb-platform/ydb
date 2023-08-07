#pragma once

#include "put_status.h"
#include "blob_constructor.h"

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/tx/columnshard/blob_manager.h>


namespace NKikimr::NColumnShard {

class TBlobPutResult : public NColumnShard::TPutStatus {
public:
    using TPtr = std::shared_ptr<TBlobPutResult>;

    TBlobPutResult(NKikimrProto::EReplyStatus status,
        NColumnShard::TBlobBatch&& blobBatch,
        THashSet<ui32>&& yellowMoveChannels,
        THashSet<ui32>&& yellowStopChannels,
        const NColumnShard::TUsage& resourceUsage)
        : BlobBatch(std::move(blobBatch))
        , ResourceUsage(resourceUsage)
    {
        SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
    }

    TBlobPutResult(NKikimrProto::EReplyStatus status) {
        SetPutStatus(status);
    }

    NColumnShard::TBlobBatch&& ReleaseBlobBatch() {
        return std::move(BlobBatch);
    }

    void AddResources(const NColumnShard::TUsage& usage) {
        ResourceUsage.Add(usage);
    }

    TAutoPtr<TCpuGuard> StartCpuGuard() {
        return new TCpuGuard(ResourceUsage);
    }

private:
    YDB_READONLY_DEF(NColumnShard::TBlobBatch, BlobBatch);
    YDB_READONLY_DEF(NColumnShard::TUsage, ResourceUsage);
};

class IWriteController {
public:
    using TPtr = std::shared_ptr<IWriteController>;
    virtual ~IWriteController() {}

    void OnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) {
        putResult->AddResources(ResourceUsage);
        DoOnReadyResult(ctx, putResult);
    }

    virtual NOlap::IBlobConstructor::TPtr GetBlobConstructor() = 0;

private:
    virtual void DoOnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) = 0;
protected:
    TUsage ResourceUsage;
};

}
