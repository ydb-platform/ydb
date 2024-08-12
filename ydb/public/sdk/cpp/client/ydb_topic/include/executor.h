#pragma once

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

namespace NYdb::NTopic {

class IExecutor: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IExecutor>;
    using TFunction = std::function<void()>;

    // Is executor asynchronous.
    virtual bool IsAsync() const = 0;

    // Post function to execute.
    virtual void Post(TFunction&& f) = 0;

    // Start method.
    // This method is idempotent.
    // It can be called many times. Only the first one has effect.
    void Start() {
        with_lock(StartLock) {
            if (!Started) {
                DoStart();
                Started = true;
            }
        }
    }

private:
    virtual void DoStart() = 0;

private:
    bool Started = false;
    TAdaptiveLock StartLock;
};

// Thread pool is expected to have been started.
IExecutor::TPtr CreateThreadPoolExecutorAdapter(std::shared_ptr<IThreadPool> threadPool);

IExecutor::TPtr CreateThreadPoolExecutor(size_t threads);

IExecutor::TPtr CreateSyncExecutor();

}  // namespace NYdb::NTopic
