#pragma once
#include <util/generic/ptr.h>
#include <util/thread/pool.h>

#include <functional>
#include <memory>
#include <vector>

namespace NPersQueue {
class TQueuePool {
public:
    void Start(size_t queuesCount);
    void Stop();

    // get one-thread-queue for processing specified tag (address)
    IThreadPool& GetQueue(const void* tag);

    const std::shared_ptr<IThreadPool>& GetQueuePtr(const void* tag);

private:
    std::vector<std::shared_ptr<IThreadPool>> Queues;
};
} // namespace NPersQueue
