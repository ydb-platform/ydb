#pragma once
#include "gc.h"

#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TGarbageCollectionActor: public TActorBootstrapped<TGarbageCollectionActor> {
private:
    const NActors::TActorId TabletActorId;
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> Requests;
    std::shared_ptr<TGCTask> GCTask;
    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr& ev);
public:
    TGarbageCollectionActor(const std::shared_ptr<TGCTask>& task, THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>>&& requests, const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId)
        , Requests(std::move(requests))
        , GCTask(task)
    {

    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("actor", "TGarbageCollectionActor")("event", "starting");
        for (auto&& i : Requests) {
            SendToBSProxy(ctx, i.first, i.second.release());
        }
        Become(&TGarbageCollectionActor::StateWork);
    }
};

}
