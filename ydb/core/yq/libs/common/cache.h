#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <util/system/mutex.h>

namespace NYq { 

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

template<typename TKey, typename TValue>
class TTtlCache {
public:
    TTtlCache(const TTtlCacheSettings& config = TTtlCacheSettings())
        : Config(config)
    { }

    void Put(const TKey& key, const TMaybe<TValue>& value) {
        TGuard<TMutex> lock(Mutex);
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
        TGuard<TMutex> lock(Mutex);
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
        return Data.size();
    }

private:
    void DropOld(const TGuard<TMutex>& ) {
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

    TMutex Mutex;

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
    THashMap<TKey, TCacheObject> Data;

    TTtlCacheSettings Config;
};

} // namespace NYq 
