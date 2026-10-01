#pragma once

#include <ydb/library/actors/core/subsystems/allocation_cache_tls.h>
#include <ydb/library/actors/core/thread_context.h>

namespace NActors {

// Test-only binding. Borrows the prior table and overrides one family. The cache
// and prior bindings must outlive this guard; all access stays on its owner thread.
template<class TTag>
class TScopedAllocationCache : TNonCopyable {
public:
    explicit TScopedAllocationCache(TAllocationCache<TTag>* cache)
        : PreviousContext(TlsThreadContext)
        , PreviousCaches(TAllocationCacheWorker::GetCurrent())
        , BoundCaches(PreviousCaches)
    {
        BoundCaches.Bind(cache);
        if (PreviousContext) {
            TAllocationCacheWorker::SetCurrent(&BoundCaches);
        } else {
            OwnedContext = std::make_unique<TThreadContext>(0, nullptr, nullptr);
            TlsThreadContext = OwnedContext.get();
            TAllocationCacheWorker::SetCurrent(&BoundCaches);
        }
    }

    ~TScopedAllocationCache() {
        if (PreviousContext) {
            TAllocationCacheWorker::SetCurrent(PreviousCaches);
        } else {
            TAllocationCacheWorker::SetCurrent(PreviousCaches);
            TlsThreadContext = nullptr;
        }
    }

private:
    TThreadContext* PreviousContext;
    TAllocationCacheWorker* PreviousCaches;
    TAllocationCacheWorker BoundCaches;
    std::unique_ptr<TThreadContext> OwnedContext;
};

} // namespace NActors
