#include "grpc_async_executor.h"

#include <algorithm>

namespace NKvVolumeStress {

TGrpcAsyncExecutor::TGrpcAsyncExecutor(ui32 threadCount)
    : ThreadCount_(std::max<ui32>(1, threadCount))
{
    CompletionQueues_.reserve(ThreadCount_);
    PollerThreads_.reserve(ThreadCount_);

    try {
        for (ui32 i = 0; i < ThreadCount_; ++i) {
            CompletionQueues_.push_back(std::make_unique<grpc::CompletionQueue>());
        }

        for (ui32 i = 0; i < ThreadCount_; ++i) {
            grpc::CompletionQueue* completionQueue = CompletionQueues_[i].get();
            PollerThreads_.emplace_back([this, completionQueue] {
                PollLoop(completionQueue);
            });
        }
    } catch (...) {
        for (const auto& completionQueue : CompletionQueues_) {
            if (completionQueue) {
                completionQueue->Shutdown();
            }
        }
        for (auto& thread : PollerThreads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        throw;
    }
}

TGrpcAsyncExecutor::~TGrpcAsyncExecutor() {
    for (const auto& completionQueue : CompletionQueues_) {
        if (completionQueue) {
            completionQueue->Shutdown();
        }
    }

    for (auto& thread : PollerThreads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

ui32 TGrpcAsyncExecutor::ThreadCount() const {
    return ThreadCount_;
}

void TGrpcAsyncExecutor::TBlockingTag::OnDone(bool ok) {
    std::lock_guard lock(Mutex_);
    Done_ = true;
    Ok_ = ok;
    Cv_.notify_one();
}

bool TGrpcAsyncExecutor::TBlockingTag::Wait() {
    std::unique_lock lock(Mutex_);
    Cv_.wait(lock, [this] { return Done_; });
    return Ok_;
}

grpc::CompletionQueue* TGrpcAsyncExecutor::NextQueue() {
    const ui64 index = NextQueueIndex_.fetch_add(1, std::memory_order_relaxed);
    return CompletionQueues_[static_cast<size_t>(index % CompletionQueues_.size())].get();
}

void TGrpcAsyncExecutor::PollLoop(grpc::CompletionQueue* completionQueue) {
    void* tagPtr = nullptr;
    bool ok = false;

    while (completionQueue->Next(&tagPtr, &ok)) {
        if (!tagPtr) {
            continue;
        }

        static_cast<ICompletionTag*>(tagPtr)->OnDone(ok);
    }
}

} // namespace NKvVolumeStress
