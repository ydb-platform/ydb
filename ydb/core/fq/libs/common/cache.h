#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/system/spinlock.h>

namespace NFq {

struct TTtlCacheSettings {
    TDuration Ttl = TDuration::Minutes(10);
    TDuration ErrorTtl = TDuration::Minutes(10);
    ui64 MaxSize = 1000000;
    bool TouchOnGet = false;

    TTtlCacheSettings& SetTtl(TDuration ttl) {
        Ttl = ttl;
        return *this;
    }

    TTtlCacheSettings& SetErrorTtl(TDuration ttl) {
        ErrorTtl = ttl;
        return *this;
    }

    TTtlCacheSettings& SetMaxSize(ui64 maxSize) {
        MaxSize = maxSize;
        return *this;
    }

    TTtlCacheSettings& SetTouchOnGet(bool flag) {
        TouchOnGet = flag;
        return *this;
    }
};

template<typename TKey, typename TValue, template <typename... Args> class TContainer = THashMap>
class TTtlCache {
public:
    TTtlCache(const TTtlCacheSettings& config = TTtlCacheSettings())
        : Config(config)
    { }

    void Put(const TKey& key, const TMaybe<TValue>& value) {
        const auto now = TInstant::Now();
        auto* queue = value.Empty()
            ? &ErrorQueue
            : &OkQueue;
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        auto [it, inserted] = Data.try_emplace(key, value, queue->end(), queue);
        if (inserted) {
            it->second.Position = queue->emplace(queue->end(), it->first, now);
        } else {
            it->second.Endpoint = value;
            auto* oldQueue = std::exchange(it->second.Queue, queue);
            it->second.Position->LastAccess = now;
            queue->splice(queue->end(), *oldQueue, it->second.Position);
        }
        DropOld(lock, now);
    }

    bool Get(const TKey& key, TMaybe<TValue>* value) {
        const auto now = TInstant::Now();
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        DropOld(lock, now);
        auto it = Data.find(key);
        if (it == Data.end()) {
            return false;
        }
        *value = it->second.Endpoint;

        if (Config.TouchOnGet) {
            it->second.Position->LastAccess = now;
            auto& queue = *it->second.Queue;
            queue.splice(queue.end(), queue, it->second.Position);
        }

        return true;
    }

    ui64 Size() const {
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        return Data.size();
    }

private:
    void DropOld(const TGuard<TAdaptiveLock>&, TInstant now) {
        auto cleanUp = [&](TList<TKeyAndTime>& queue, const TDuration& ttl) {
            for (auto it = queue.begin(); it != queue.end() && (it->LastAccess <= now - ttl || Data.size() > Config.MaxSize) ; ) {
                Data.erase(it->Key);
                it = queue.erase(it);
            }
        };

        cleanUp(ErrorQueue, Config.ErrorTtl);
        cleanUp(OkQueue, Config.Ttl);
    }

    TAdaptiveLock AdaptiveLock;

    struct TKeyAndTime {
        TKey Key;
        TInstant LastAccess;
    };
    TList<TKeyAndTime> OkQueue;
    TList<TKeyAndTime> ErrorQueue;

    struct TCacheObject {
        TMaybe<TValue> Endpoint;
        typename TList<TKeyAndTime>::iterator Position;
        TList<TKeyAndTime>* Queue;
    };
    TContainer<TKey, TCacheObject> Data;

    TTtlCacheSettings Config;
};

} // namespace NFq
