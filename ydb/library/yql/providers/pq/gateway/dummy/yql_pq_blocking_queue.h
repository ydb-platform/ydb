#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/deque.h>
#include <util/system/condvar.h>

namespace NYql {

template<typename TEvent>
class TBlockingEQueue {
public:
    explicit TBlockingEQueue(size_t maxSize)
        : MaxSize_(maxSize)
    {
    }
    void Push(TEvent&& e, size_t size = 0) {
        with_lock(Mutex_) {
            CanPush_.WaitI(Mutex_, [this] () {return CanPushPredicate();});
            Events_.emplace_back(std::move(e), size );
            Size_ += size;
        }
        CanPop_.BroadCast();
    }

    void BlockUntilEvent() {
        with_lock(Mutex_) {
            CanPop_.WaitI(Mutex_, [this] () {return CanPopPredicate();});
        }
    }

    std::optional<TEvent> Pop(bool block) {
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

    void Stop() {
        with_lock(Mutex_) {
            Stopped_ = true;
            CanPop_.BroadCast();
            CanPush_.BroadCast();
        }
    }

    bool IsStopped() {
        with_lock(Mutex_) {
            return Stopped_;
        }
    }

private:
    bool CanPopPredicate() const {
        return !Events_.empty() || Stopped_;
    }

    bool CanPushPredicate() const {
        return Size_ < MaxSize_ || Stopped_;
    }

    size_t MaxSize_;
    size_t Size_ = 0;
    TDeque<std::pair<TEvent, size_t>> Events_;
    bool Stopped_ = false;
    TMutex Mutex_;
    TCondVar CanPop_;
    TCondVar CanPush_;
};

}
