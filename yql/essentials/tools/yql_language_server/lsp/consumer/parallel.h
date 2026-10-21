#pragma once

#include "rw_binary_semaphore.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/blocking_queue.h>
#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/listener.h>

#include <util/thread/pool.h>
#include <util/system/rwlock.h>

namespace NLsp {

namespace NDetail {

template <typename T, std::invocable<const T&> P>
class TParallelConsumer final: public IConsumer<T> {
public:
    TParallelConsumer(THolder<IThreadPool> pool, P isPure, typename IConsumer<T>::TPtr consumer)
        : Consumer_(std::move(consumer))
        , IsPure_(std::move(isPure))
        , Pool_(std::move(pool))
    {
    }

    void Receive(T value) override {
        if (IsPure_(static_cast<const T&>(value))) {
            TReadGuardBase lock(Scheduler_);
            Submit(std::move(value), std::move(lock));
        } else {
            TWriteGuardBase lock(Scheduler_);
            Submit(std::move(value), std::move(lock));
        }
    }

    void Stop() override {
        TWriteGuardBase lock(Scheduler_);
        Consumer_->Stop();
    }

private:
    void Submit(T value, TWriteGuardBase<TRWBinarySemaphore> lock) {
        Y_UNUSED(lock);
        Consumer_->Receive(std::move(value));
    }

    void Submit(T value, TReadGuardBase<TRWBinarySemaphore> lock) {
        Pool_->SafeAddFunc([value = std::move(value),
                            consumer = Consumer_,
                            lock = std::move(lock)]() mutable {
            consumer->Receive(std::move(value));
        });
    }

    TRWBinarySemaphore Scheduler_;
    typename IConsumer<T>::TPtr Consumer_;
    P IsPure_;
    THolder<IThreadPool> Pool_; // must be destructed (joined) before the Scheduler_
};

} // namespace NDetail

template <typename T, std::invocable<const T&> P>
IConsumer<T>::TPtr Parallel(THolder<IThreadPool> pool, P isPure, typename IConsumer<T>::TPtr consumer) {
    return new NDetail::TParallelConsumer<T, P>(std::move(pool), std::move(isPure), std::move(consumer));
}

} // namespace NLsp
