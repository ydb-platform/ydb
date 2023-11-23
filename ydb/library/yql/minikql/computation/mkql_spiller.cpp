#include "mkql_spiller.h"
#include <library/cpp/threading/future/core/future.h>
#include <util/system/thread.h>
#include <unordered_map>

namespace NKikimr::NMiniKQL {

//Dummy synchronous in-memory spiller
class TDummySpiller: public ISpiller{
public:
    TDummySpiller()
        : NextKey(0)
    {}

    NThreading::TFuture<TKey> Put(TRope&& blob) override {
        auto promise = NThreading::NewPromise<ISpiller::TKey>();
//        TThread t([this, blob = std::move(blob), p = std::move(p)]() {
//            WriteAsync(blob, p);
//        });
//        t.Detach();
//        return f;
        auto key = NextKey;
        Storage[key] = blob;
        NextKey++;
        promise.SetValue(key);
        return promise.GetFuture();;
    }
    std::optional<NThreading::TFuture<TRope>> Get(TKey key) override {
        auto promise = NThreading::NewPromise<TRope>();
        if (auto it = Storage.find(key); it != Storage.end()) {
            promise.SetValue(it->second);
            return promise.GetFuture();;
        } else {
            return std::nullopt;
        }
    }
    std::optional<NThreading::TFuture<TRope>> Extract(TKey key) override {
        auto promise = NThreading::NewPromise<TRope>();
        if (auto it = Storage.find(key); it != Storage.end()) {
            promise.SetValue(std::move(it->second));
            Storage.erase(it);
            return promise.GetFuture();;
        } else {
            return std::nullopt;
        }
    }
    NThreading::TFuture<void> Delete(TKey key) override {
        auto promise = NThreading::NewPromise<void>();
        promise.SetValue();
        Storage.erase(key);
        return promise.GetFuture();
    }
private:
    ISpiller::TKey NextKey;
    std::unordered_map<ISpiller::TKey, TRope> Storage;
};
ISpiller::TPtr MakeSpiller() {
    return std::make_shared<TDummySpiller>();
}

} //namespace NKikimr::NMiniKQL
