#include "async_semaphore.h"

#include <util/system/guard.h>
#include <util/system/yassert.h>

namespace NYql {

TAsyncSemaphore::TAsyncSemaphore(size_t count)
    : Count_(count)
{
    Y_ASSERT(count > 0);
}

TAsyncSemaphore::TPtr TAsyncSemaphore::Make(size_t count) {
    return TPtr(new TAsyncSemaphore(count));
}

NThreading::TFuture<TAsyncSemaphore::TPtr> TAsyncSemaphore::AcquireAsync() {
    with_lock(Lock_) {
        if (Count_) {
            --Count_;
            return NThreading::MakeFuture<TAsyncSemaphore::TPtr>(this);
        }
        auto promise = NThreading::NewPromise<TAsyncSemaphore::TPtr>();
        Promises_.push_back(promise);
        return promise.GetFuture();
    }
}

void TAsyncSemaphore::Release() {
    NThreading::TPromise<TPtr> promise;
    with_lock(Lock_) {
        if (Promises_.empty()) {
            ++Count_;
            return;
        } else {
            promise = Promises_.front();
            Promises_.pop_front();
        }
    }
    promise.SetValue(this);
}

void TAsyncSemaphore::Cancel() {
    std::list<NThreading::TPromise<TPtr>> promises;
    with_lock(Lock_) {
        std::swap(Promises_, promises);
    }
    for (auto& p: promises) {
        p.SetException("Cancelled");
    }
}

TAsyncSemaphore::TAutoRelease::~TAutoRelease() {
    if (Sem) {
        Sem->Release();
    }
}

std::function<void (const NThreading::TFuture<void>&)> TAsyncSemaphore::TAutoRelease::DeferRelease() {
    return [s = std::move(this->Sem)](const NThreading::TFuture<void>& f) {
        f.GetValue();
        s->Release();
    };
}


std::function<void (const NThreading::TFuture<void>&)> TAsyncSemaphore::DeferRelease() {
    return [s = TPtr(this)](const NThreading::TFuture<void>& f) {
        f.GetValue();
        s->Release();
    };
}

}
