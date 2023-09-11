#pragma once

#include "put_status.h"
#include "blob_constructor.h"

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract.h>


namespace NKikimr::NColumnShard {

class TBlobPutResult : public NColumnShard::TPutStatus {
public:
    using TPtr = std::shared_ptr<TBlobPutResult>;

    TBlobPutResult(NKikimrProto::EReplyStatus status,
        THashSet<ui32>&& yellowMoveChannels,
        THashSet<ui32>&& yellowStopChannels,
        const NColumnShard::TUsage& resourceUsage)
        : ResourceUsage(resourceUsage)
    {
        SetPutStatus(status, std::move(yellowMoveChannels), std::move(yellowStopChannels));
    }

    TBlobPutResult(NKikimrProto::EReplyStatus status) {
        SetPutStatus(status);
    }

    void AddResources(const NColumnShard::TUsage& usage) {
        ResourceUsage.Add(usage);
    }

    TAutoPtr<TCpuGuard> StartCpuGuard() {
        return new TCpuGuard(ResourceUsage);
    }

private:
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

    virtual void OnBlobWriteResult(const TEvBlobStorage::TEvPutResult& result) = 0;
    virtual std::optional<NOlap::TBlobWriteInfo> Next() = 0;
    virtual bool IsBlobActionsReady() const = 0;
    virtual std::vector<std::shared_ptr<NOlap::IBlobsAction>> GetBlobActions() const = 0;
private:
    virtual void DoOnReadyResult(const NActors::TActorContext& ctx, const TBlobPutResult::TPtr& putResult) = 0;
protected:
    TUsage ResourceUsage;
};

}
