#include "yql_pq_dummy_gateway.h"

#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <util/generic/deque.h>
#include <util/system/condvar.h>

namespace NYql {

class TBlockingEQueue {
public:
    TBlockingEQueue(size_t maxSize);
    void Push(NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size);
    void BlockUntilEvent();
    TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> Pop(bool block);
    void Stop();
    bool IsStopped();

private:
    bool CanPopPredicate();

    size_t MaxSize_;
    size_t Size_ = 0;
    TDeque<std::pair<NYdb::NTopic::TReadSessionEvent::TEvent, size_t>> Events_;
    bool Stopped_ = false;
    TMutex Mutex_;
    TCondVar CanPop_;
    TCondVar CanPush_;
};

}