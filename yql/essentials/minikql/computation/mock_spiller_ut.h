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

    const std::vector<size_t>& GetPutSizes() const {
        return PutSizes_;
    }
private:
    ISpiller::TKey NextKey_;
    std::unordered_map<ISpiller::TKey, NYql::TChunkedBuffer> Storage_;
    std::vector<size_t> PutSizes_;
};
inline ISpiller::TPtr CreateMockSpiller() {
    return std::make_shared<TMockSpiller>();
}

} //namespace NKikimr::NMiniKQL
