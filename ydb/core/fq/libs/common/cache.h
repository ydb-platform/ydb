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
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        auto it = Data.find(key);
        if (it != Data.end()) {
            it->second.Queue->erase(it->second.Position);
        }

        auto* queue = value.Empty()
            ? &ErrorQueue
            : &OkQueue;
        queue->push_back({key, TInstant::Now()});
        auto position = queue->end();
        --position;
        TCacheObject object = {value, position, queue};
        Data[key] = object;
        DropOld(lock);
    }

    bool Get(const TKey& key, TMaybe<TValue>* value) {
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        DropOld(lock);
        auto it = Data.find(key);
        if (it == Data.end()) {
            return false;
        }
        *value = it->second.Endpoint;

        if (Config.TouchOnGet) {
            it->second.Queue->erase(it->second.Position);
            it->second.Queue->push_back({key, TInstant::Now()});
            it->second.Position = it->second.Queue->end();
            --it->second.Position;
        }

        return true;
    }

    ui64 Size() const {
        TGuard<TAdaptiveLock> lock(AdaptiveLock);
        return Data.size();
    }

private:
    void DropOld(const TGuard<TAdaptiveLock>&) {
        auto now = TInstant::Now();
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
