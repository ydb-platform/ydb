#include "queue_pool.h"

#include <util/digest/city.h>

namespace NPersQueue {
void TQueuePool::Start(size_t queuesCount) {
    Y_VERIFY(queuesCount > 0);
    Queues.resize(queuesCount);
    for (auto& queuePtr : Queues) {
        queuePtr = std::make_shared<TThreadPool>();
        queuePtr->Start(1); // start one thread for each tag
    }
}

void TQueuePool::Stop() {
    for (auto& queuePtr : Queues) {
        queuePtr->Stop();
    }
    Queues.clear();
}

const std::shared_ptr<IThreadPool>& TQueuePool::GetQueuePtr(const void* tag) {
    Y_VERIFY(!Queues.empty());
    const size_t queue = static_cast<size_t>(CityHash64(reinterpret_cast<const char*>(&tag), sizeof(tag))) % Queues.size();
    return Queues[queue];
}

IThreadPool& TQueuePool::GetQueue(const void* tag) {
    return *GetQueuePtr(tag);
}
} // namespace NPersQueue
