#pragma once

#include <util/generic/hash.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <memory>

namespace NKikimr::NOlap {

template <typename TKey, typename TObject>
class TObjectCache : std::enable_shared_from_this<TObjectCache<TKey, TObject>> {
private:
    THashMap<TKey, std::weak_ptr<const TObject>> Objects;
    mutable TMutex Mutex;

public:
    class TEntryGuard {
    private:
        TKey Key;
        std::shared_ptr<const TObject> Object;
        std::weak_ptr<TObjectCache> Cache;

    public:
        TEntryGuard(TKey key, const std::shared_ptr<const TObject> object, TObjectCache* cache)
            : Key(key)
            , Object(object)
            , Cache(cache ? cache->weak_from_this() : std::weak_ptr<TObjectCache>()) {
        }

        const TObject* operator->() const {
            return Object.get();
        }
        const TObject& operator*() const {
            return *Object;
        }

        ~TEntryGuard() {
            Object.reset();
            if (auto cache = Cache.lock()) {
                cache->TryFree(Key);
            }
        }
    };

public:
    TEntryGuard Upsert(TKey key, TObject&& object) {
        TGuard lock(Mutex);
        auto* findSchema = Objects.FindPtr(key);
        std::shared_ptr<const TObject> cachedObject;
        if (findSchema) {
            cachedObject = findSchema->lock();
        }
        if (!cachedObject) {
            cachedObject = std::make_shared<const TObject>(std::move(object));
            Objects[key] = cachedObject;
        }
        return TEntryGuard(std::move(key), cachedObject, this);
    }

    void TryFree(const TKey& key) {
        TGuard lock(Mutex);
        auto findObject = Objects.FindPtr(key);
        if (findObject) {
            if (findObject->expired()) {
                Objects.erase(key);
            }
        }
    }
};

}   // namespace NKikimr::NOlap
