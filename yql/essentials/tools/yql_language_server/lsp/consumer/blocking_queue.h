#pragma once

#include "base.h"

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/system/type_name.h>

namespace NLsp {

template <typename T>
using TBlockingQueue = NThreading::TBlockingQueue<T>;

template <typename T>
using TBlockingQueuePtr = std::shared_ptr<TBlockingQueue<T>>;

namespace NDetail {

template <typename T>
class TBlockingQueueConsumer final: public IConsumer<T> {
public:
    explicit TBlockingQueueConsumer(TBlockingQueuePtr<T> queue)
        : Queue_(std::move(queue))
    {
    }

    void Receive(T value) override {
        if (!Queue_->Push(std::move(value))) {
            throw yexception() << "queue rejected a value of type " << TypeName(typeid(T));
        }
    }

    void Stop() override {
        Queue_->Stop();
    }

private:
    TBlockingQueuePtr<T> Queue_;
};

} // namespace NDetail

template <typename T>
IConsumer<T>::TPtr Consumer(TBlockingQueuePtr<T> queue) {
    return new NDetail::TBlockingQueueConsumer<T>(std::move(queue));
}

} // namespace NLsp
