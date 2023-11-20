#pragma once

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <array>

namespace NPrivate {
    template <typename T, typename THash>
    concept CHashableBy = requires (const THash& hash, const T& t) {
        hash(t);
    };
}

template <typename K, typename V, size_t BucketCount = 64, typename L = TAdaptiveLock>
class TConcurrentHashMap {
public:
    using TActualMap = THashMap<K, V>;
    using TLock = L;

    struct TBucket {
        friend class TConcurrentHashMap;

    private:
        TActualMap Map;
        mutable TLock Mutex;

    public:
        TLock& GetMutex() const {
            return Mutex;
        }

        TActualMap& GetMap() {
            return Map;
        }
        const TActualMap& GetMap() const {
            return Map;
        }

        const V& GetUnsafe(const K& key) const {
            typename TActualMap::const_iterator it = Map.find(key);
            Y_ABORT_UNLESS(it != Map.end(), "not found by key");
            return it->second;
        }

        V& GetUnsafe(const K& key) {
            typename TActualMap::iterator it = Map.find(key);
            Y_ABORT_UNLESS(it != Map.end(), "not found by key");
            return it->second;
        }

        V RemoveUnsafe(const K& key) {
            typename TActualMap::iterator it = Map.find(key);
            Y_ABORT_UNLESS(it != Map.end(), "removing non-existent key");
            V r = std::move(it->second);
            Map.erase(it);
            return r;
        }

        bool HasUnsafe(const K& key) const {
            typename TActualMap::const_iterator it = Map.find(key);
            return (it != Map.end());
        }

        const V* TryGetUnsafe(const K& key) const {
            typename TActualMap::const_iterator it = Map.find(key);
            return it == Map.end() ? nullptr : &it->second;
        }

        V* TryGetUnsafe(const K& key) {
            typename TActualMap::iterator it = Map.find(key);
            return it == Map.end() ? nullptr : &it->second;
        }
    };

    std::array<TBucket, BucketCount> Buckets;

public:
    template <NPrivate::CHashableBy<THash<K>> TKey>
    TBucket& GetBucketForKey(const TKey& key) {
        return Buckets[THash<K>()(key) % BucketCount];
    }

    template <NPrivate::CHashableBy<THash<K>> TKey>
    const TBucket& GetBucketForKey(const TKey& key) const {
        return Buckets[THash<K>()(key) % BucketCount];
    }

    void Insert(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        bucket.Map[key] = value;
    }

    void InsertUnique(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        if (!bucket.Map.insert(std::make_pair(key, value)).second) {
            Y_ABORT("non-unique key");
        }
    }

    V& InsertIfAbsent(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        return bucket.Map.insert(std::make_pair(key, value)).first->second;
    }

    template <typename TKey, typename... Args>
    V& EmplaceIfAbsent(TKey&& key, Args&&... args) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        if (V* value = bucket.TryGetUnsafe(key)) {
            return *value;
        }
        return bucket.Map.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(std::forward<TKey>(key)),
            std::forward_as_tuple(std::forward<Args>(args)...)
        ).first->second;
    }

    template <typename Callable>
    V& InsertIfAbsentWithInit(const K& key, Callable initFunc) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        if (V* value = bucket.TryGetUnsafe(key)) {
            return *value;
        }

        return bucket.Map.insert(std::make_pair(key, initFunc())).first->second;
    }

    V Get(const K& key) const {
        const TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        return bucket.GetUnsafe(key);
    }

    bool Get(const K& key, V& result) const {
        const TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        if (const V* value = bucket.TryGetUnsafe(key)) {
            result = *value;
            return true;
        }
        return false;
    }

    V Remove(const K& key) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        return bucket.RemoveUnsafe(key);
    }

    bool Has(const K& key) const {
        const TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        return bucket.HasUnsafe(key);
    }
};
