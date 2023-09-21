#pragma once
#include "gc.h"

#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TGarbageCollectionActor: public TActorBootstrapped<TGarbageCollectionActor> {
private:
    const NActors::TActorId TabletActorId;
    std::shared_ptr<TGCTask> GCTask;

    THashSet<TLogoBlobID> BlobIdsToRemove;
    void Handle(NWrappers::NExternalStorage::TEvDeleteObjectResponse::TPtr& ev);
public:
    TGarbageCollectionActor(const std::shared_ptr<TGCTask>& task, const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId)
        , GCTask(task)
    {

    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvDeleteObjectResponse, Handle);
        }
    }

    void Bootstrap(const TActorContext& ctx);
};

}
