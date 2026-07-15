#include "async_queue.h"

namespace NYql {

TAsyncQueue::TAsyncQueue(size_t numThreads, const TString& poolName) {
    if (1 == numThreads) {
        MtpQueue_.Reset(new TFakeThreadPool());
    } else {
        MtpQueue_.Reset(new TSimpleThreadPool(TThreadPoolParams{poolName}));
    }
    MtpQueue_->Start(numThreads);
}

TAsyncQueue::TPtr TAsyncQueue::Make(size_t numThreads, const TString& poolName) {
    class TAsyncQueueImpl: public TAsyncQueue {
    public:
        TAsyncQueueImpl(size_t numThreads, const TString& poolName)
            : TAsyncQueue(numThreads, poolName)
        {
        }
    };
    return std::make_shared<TAsyncQueueImpl>(numThreads, poolName);
}

} // namespace NYql
