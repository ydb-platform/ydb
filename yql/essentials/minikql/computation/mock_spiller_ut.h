#pragma once

#include <unordered_map>

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

namespace NKikimr::NMiniKQL {

//Dummy synchronous in-memory spiller
class TMockSpiller: public ISpiller{
public:
    TMockSpiller()
        : NextKey_(0)
    {}

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        auto promise = NThreading::NewPromise<ISpiller::TKey>();

        auto key = NextKey_;
        Storage_[key] = std::move(blob);
        PutSizes_.push_back(Storage_[key].Size());
        NextKey_++;
        promise.SetValue(key);
        return promise.GetFuture();;
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

    void ReportAlloc(size_t size) override {
        AllocatedMemory_ += size;
        AllocCalls_.push_back(size);
    }

    void ReportFree(size_t size) override {
        FreedMemory_ += size;
        FreeCalls_.push_back(size);
    }

    const std::vector<size_t>& GetPutSizes() const {
        return PutSizes_;
    }

    const std::vector<size_t>& GetAllocCalls() const {
        return AllocCalls_;
    }

    const std::vector<size_t>& GetFreeCalls() const {
        return FreeCalls_;
    }

    size_t GetAllocatedMemory() const {
        return AllocatedMemory_;
    }

    size_t GetFreedMemory() const {
        return FreedMemory_;
    }
private:
    ISpiller::TKey NextKey_;
    std::unordered_map<ISpiller::TKey, NYql::TChunkedBuffer> Storage_;
    std::vector<size_t> PutSizes_;
    std::vector<size_t> AllocCalls_;
    std::vector<size_t> FreeCalls_;
    size_t AllocatedMemory_ = 0;
    size_t FreedMemory_ = 0;
};
inline ISpiller::TPtr CreateMockSpiller() {
    return std::make_shared<TMockSpiller>();
}

} //namespace NKikimr::NMiniKQL
