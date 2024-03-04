#pragma once

#include "task.h"
#include "events.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TActor: public TActorBootstrapped<TActor> {
private:
    std::shared_ptr<ITask> Task;
public:
    static TAtomicCounter WaitingBlobsCount;
    TActor(const std::shared_ptr<ITask>& task);

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev);

    void Bootstrap();

    STFUNC(StateWait) {
        TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD);
        switch (ev->GetTypeRewrite()) {
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            default:
                AFL_VERIFY(false);
        }
    }

    ~TActor();
};

}
