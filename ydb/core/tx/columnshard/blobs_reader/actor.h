#pragma once

#include "task.h"
#include "events.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TActor : public TActorBootstrapped<TActor> {
private:
    ui64 TabletId;
    NActors::TActorId Parent;
    NActors::TActorId BlobCacheActorId;
    THashMap<TBlobRange, std::vector<std::shared_ptr<ITask>>> BlobTasks;

public:
    TActor(ui64 tabletId, const TActorId& parent);

    void Handle(TEvStartReadTask::TPtr& ev);
    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev);

    void Bootstrap() {
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        TLogContextGuard gLogging(NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", Parent));
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStartReadTask, Handle);
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            default:
                break;
        }
    }

};

}
