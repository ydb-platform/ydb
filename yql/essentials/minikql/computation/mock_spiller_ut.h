#pragma once

#include <unordered_map>

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

namespace NKikimr::NMiniKQL {

//Dummy synchronous in-memory spiller
class TMockSpiller: public ISpiller{
public:
    TMockSpiller()
        : NextKey(0)
    {}

    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override {
        auto promise = NThreading::NewPromise<ISpiller::TKey>();

        auto key = NextKey;
        Storage[key] = std::move(blob);
        NextKey++;
        promise.SetValue(key);
        return promise.GetFuture();;
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        if (auto it = Storage.find(key); it != Storage.end()) {
            promise.SetValue(it->second);
        } else {
            promise.SetValue(std::nullopt);
        }

        return promise.GetFuture();
    }

    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override {
        auto promise = NThreading::NewPromise<std::optional<NYql::TChunkedBuffer>>();
        if (auto it = Storage.find(key); it != Storage.end()) {
            promise.SetValue(std::move(it->second));
            Storage.erase(it);
        } else {
            promise.SetValue(std::nullopt);
        }

        return promise.GetFuture();
    }
    NThreading::TFuture<void> Delete(TKey key) override {
        auto promise = NThreading::NewPromise<void>();
        promise.SetValue();
        Storage.erase(key);
        return promise.GetFuture();
    }
private:
    ISpiller::TKey NextKey;
    std::unordered_map<ISpiller::TKey, NYql::TChunkedBuffer> Storage;
};
inline ISpiller::TPtr CreateMockSpiller() {
    return std::make_shared<TMockSpiller>();
}

} //namespace NKikimr::NMiniKQL
