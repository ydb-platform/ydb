#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>
#include <util/generic/size_literals.h>
#include <util/system/thread.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/system/guard.h>

#include <unordered_map>
#include <queue>

namespace NKikimr::NMiniKQL {

// A clone of TMockSpiller/TMockSpillerFactory that can spill into a large preallocated buffer

class TPreallocatedSpiller: public ISpiller {
public:
    TPreallocatedSpiller(TBuffer& dataStore)
        : NextKey_(0)
        , DataStore(dataStore)
        , StoreStream(DataStore)
    {
        Storage_.rehash(128 * 4); // TODO: configurable
    }

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        auto promise = NThreading::NewPromise<ISpiller::TKey>();

        auto key = NextKey_;
        NYql::TChunkedBuffer storageBuf;

        Y_ENSURE(DataStore.Size() + blob.Size() < DataStore.Capacity());

        const char* currPos = DataStore.Pos();

        blob.CopyTo(StoreStream);
        StoreStream.Flush();

        Storage_[key] = NYql::TChunkedBuffer(TStringBuf(currPos, blob.Size()), nullptr);
        PutSizes_.push_back(Storage_[key].Size());
        NextKey_++;
        promise.SetValue(key);

        blob.Clear();

        return promise.GetFuture();
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        if (auto it = Storage_.find(key); it != Storage_.end()) {
            promise.SetValue(it->second);
        } else {
            promise.SetValue(std::nullopt);
        }

        return promise.GetFuture();
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        if (auto it = Storage_.find(key); it != Storage_.end()) {
            promise.SetValue(std::move(it->second));
            Storage_.erase(it);
        } else {
            promise.SetValue(std::nullopt);
        }

        return promise.GetFuture();
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        auto promise = NThreading::NewPromise<void>();
        promise.SetValue();
        Storage_.erase(key);
        return promise.GetFuture();
    }

    void ReportAlloc(ui64 bytes) override {
        Y_UNUSED(bytes);
    }

    void ReportFree(ui64 bytes) override {
        Y_UNUSED(bytes);
    }

    const std::vector<size_t>& GetPutSizes() const {
        return PutSizes_;
    }

private:
    ISpiller::TKey NextKey_;
    std::unordered_map<ISpiller::TKey, NYql::TChunkedBuffer> Storage_;
    std::vector<size_t> PutSizes_;
    TBuffer& DataStore;
    TBufferOutput StoreStream;
};

class TPreallocatedSpillerFactory : public ISpillerFactory
{
public:
    TPreallocatedSpillerFactory(size_t dataStoreLen = 4_GB)
        : DataStore(dataStoreLen)
    {
        // prefault the buffer
        memset(DataStore.Data(), 0xFF, DataStore.Capacity());
    }

    void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& /*spillingTaskCounters*/) override {
    }

    ISpiller::TPtr CreateSpiller() override {
        auto new_spiller = std::make_shared<TPreallocatedSpiller>(DataStore);
        Spillers_.push_back(new_spiller);
        return new_spiller;
    }

    const std::vector<ISpiller::TPtr>& GetCreatedSpillers() const {
        return Spillers_;
    }

    void ForgetSpillers() {
        Spillers_.clear();
    }

    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback reportAlloc, ISpiller::TMemoryReportCallback reportFree) override {
        Y_UNUSED(reportAlloc);
        Y_UNUSED(reportFree);
    }

private:
    TBuffer DataStore;
    std::vector<ISpiller::TPtr> Spillers_;
};

// A spiller wrapper that simulates delayed async operations without actually doing file I/O
class TSlowSpiller: public ISpiller {
public:
    TSlowSpiller(ISpiller::TPtr fastSpiller)
        : FastSpiller(fastSpiller)
        , OperationCounter(0)
        , Stopped(false)
    {
        WorkerThread = std::make_unique<TThread>([this]() { this->WorkerThreadFunc(); });
        WorkerThread->Start();
    }

    ~TSlowSpiller() {
        {
            TGuard<TMutex> guard(QueueMutex);
            Stopped = true;
            QueueCondVar.Signal();
        }

        if (WorkerThread) {
            WorkerThread->Join();
        }
    }

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        auto promise = NThreading::NewPromise<ISpiller::TKey>();
        auto fastFuture = FastSpiller->Put(std::move(blob));

        EnqueueDelayedResolution([promise, fastFuture]() mutable {
            promise.SetValue(fastFuture.ExtractValueSync());
        });

        return promise.GetFuture();
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        auto fastFuture = FastSpiller->Get(key);

        EnqueueDelayedResolution([promise, fastFuture]() mutable {
            promise.SetValue(fastFuture.ExtractValueSync());
        });

        return promise.GetFuture();
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        auto fastFuture = FastSpiller->Extract(key);

        EnqueueDelayedResolution([promise, fastFuture]() mutable {
            promise.SetValue(fastFuture.ExtractValueSync());
        });

        return promise.GetFuture();
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        auto promise = NThreading::NewPromise<void>();
        auto fastFuture = FastSpiller->Delete(key);

        // Enqueue the promise to be resolved with delay
        EnqueueDelayedResolution([promise, fastFuture]() mutable {
            fastFuture.GetValueSync();
            promise.SetValue();
        });

        return promise.GetFuture();
    }

    void ReportAlloc([[maybe_unused]] ui64 bytes) override {
    }

    void ReportFree([[maybe_unused]] ui64 bytes) override {
    }

    ISpiller::TPtr GetUnderlyingSpiller() {
        return FastSpiller;
    }

private:
    void EnqueueDelayedResolution(std::function<void()> resolver) {
        TGuard<TMutex> guard(QueueMutex);
        if (!Stopped) {
            ResolverQueue.push(std::move(resolver));
            QueueCondVar.Signal();
        }
    }

    void WorkerThreadFunc() {
        while (true) {
            TGuard<TMutex> guard(QueueMutex);
            while (ResolverQueue.empty() && !Stopped) {
                QueueCondVar.WaitI(QueueMutex);
            }

            if (Stopped) {
                break;
            }

            std::function<void()> resolver = std::move(ResolverQueue.front());
            ResolverQueue.pop();

            // Every 10th operation gets 500ms delay, others get 10ms delay
            if (OperationCounter % 10 == 9) {
                Sleep(TDuration::MilliSeconds(500));
            } else {
                Sleep(TDuration::MilliSeconds(10));
            }

            resolver();
        }
    }

    ISpiller::TPtr FastSpiller;

    std::unique_ptr<TThread> WorkerThread;
    TMutex QueueMutex;
    TCondVar QueueCondVar;
    std::queue<std::function<void()>> ResolverQueue;
    ui64 OperationCounter;
    bool Stopped;
};

class TSlowSpillerFactory : public ISpillerFactory
{
public:
    TSlowSpillerFactory(std::shared_ptr<ISpillerFactory> fastSpillerFactory)
        : FastSpillerFactory(fastSpillerFactory)
    {
    }

    ISpiller::TPtr CreateSpiller() override {
        return std::make_shared<TSlowSpiller>(FastSpillerFactory->CreateSpiller());
    }

    void SetTaskCounters([[maybe_unused]] const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& spillingTaskCounters) override {
    }

    void SetMemoryReportingCallbacks([[maybe_unused]] ISpiller::TMemoryReportCallback reportAlloc, [[maybe_unused]] ISpiller::TMemoryReportCallback reportFree) override {
    }

private:
    std::shared_ptr<ISpillerFactory> FastSpillerFactory;
};

} // namespace NKikimr::NMiniKQL
