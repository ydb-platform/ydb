#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller_factory.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

#include <unordered_map>

namespace NKikimr::NMiniKQL {

// A clone of TMockSpiller/TMockSpillerFactory that can spill into a large preallocated buffer

class TPreallocatedSpiller: public ISpiller{
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
    TPreallocatedSpillerFactory()
        : DataStore(4ULL << 30) // TODO: configure
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

    void SetMemoryReportingCallbacks(ISpiller::TMemoryReportCallback reportAlloc, ISpiller::TMemoryReportCallback reportFree) override {
        Y_UNUSED(reportAlloc);
        Y_UNUSED(reportFree);
    }

private:
    TBuffer DataStore;
    std::vector<ISpiller::TPtr> Spillers_;
};

} // namespace NKikimr::NMiniKQL
