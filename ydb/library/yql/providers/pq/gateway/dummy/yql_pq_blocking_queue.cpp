#include "yql_pq_blocking_queue.h"

#include <library/cpp/threading/future/async.h>
//#include <thread>

namespace NYql {

TBlockingEQueue::TBlockingEQueue(size_t maxSize) : MaxSize_(maxSize) {
}

void TBlockingEQueue::Push(NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size) {
    with_lock(Mutex_) {
        CanPush_.WaitI(Mutex_, [this] () {return CanPushPredicate();});
        Events_.emplace_back(std::move(e), size );
        Size_ += size;
    }
    CanPop_.BroadCast();
}

void TBlockingEQueue::BlockUntilEvent() {
    with_lock(Mutex_) {
        CanPop_.WaitI(Mutex_, [this] () {return CanPopPredicate();});
    }
}

TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> TBlockingEQueue::Pop(bool block) {
    with_lock(Mutex_) {
        if (block) {
            CanPop_.WaitI(Mutex_, [this] () {return CanPopPredicate();});
        } else {
            if (!CanPopPredicate()) {
                return {};
            }
        }
        if (Events_.empty()) {
            return {};
        }

        auto [front, size] = std::move(Events_.front());
        Events_.pop_front();
        Size_ -= size;
        if (Size_ < MaxSize_) {
            CanPush_.BroadCast();
        }
        return std::move(front); // cast to TMaybe<>
    }
}

void TBlockingEQueue::Stop() {
    with_lock(Mutex_) {
        Stopped_ = true;
        CanPop_.BroadCast();
        CanPush_.BroadCast();
    }
}

bool TBlockingEQueue::IsStopped() {
    with_lock(Mutex_) {
        return Stopped_;
    }
}

bool TBlockingEQueue::CanPopPredicate() {
    return !Events_.empty() && !Stopped_;
}

bool TBlockingEQueue::CanPushPredicate() {
    return Size_ < MaxSize_ || Stopped_;
}

}
