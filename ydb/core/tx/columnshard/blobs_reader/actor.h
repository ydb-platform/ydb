#pragma once

#include "retry_state.h"
#include "task.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

class TActor: public TActorBootstrapped<TActor> {
private:
    std::shared_ptr<ITask> Task;
    TRetryState RetryState;

    void HandleRetryTimer();

public:
    static TAtomicCounter WaitingBlobsCount;
    TActor(const std::shared_ptr<ITask>& task);

    void Handle(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult::TPtr& ev);

    void Bootstrap();

    STFUNC(StateWait) {
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
            {"eventType", ev->GetTypeName()});
        switch (ev->GetTypeRewrite()) {
            hFunc(NBlobCache::TEvBlobCache::TEvReadBlobRangeResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleRetryTimer);
            default:
                AFL_VERIFY(false);
        }
    }

    ~TActor();
};

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
