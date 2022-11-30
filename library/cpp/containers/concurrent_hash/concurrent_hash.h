#pragma once

#include <util/generic/hash.h>
#include <util/system/spinlock.h>

#include <array>

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
            Y_VERIFY(it != Map.end(), "not found by key");
            return it->second;
        }

        V& GetUnsafe(const K& key) {
            typename TActualMap::iterator it = Map.find(key);
            Y_VERIFY(it != Map.end(), "not found by key");
            return it->second;
        }

        V RemoveUnsafe(const K& key) {
            typename TActualMap::iterator it = Map.find(key);
            Y_VERIFY(it != Map.end(), "removing non-existent key");
            V r = std::move(it->second);
            Map.erase(it);
            return r;
        }

        bool HasUnsafe(const K& key) const {
            typename TActualMap::const_iterator it = Map.find(key);
            return (it != Map.end());
        }
    };

    std::array<TBucket, BucketCount> Buckets;

public:
    TBucket& GetBucketForKey(const K& key) {
        return Buckets[THash<K>()(key) % BucketCount];
    }

    const TBucket& GetBucketForKey(const K& key) const {
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
            Y_FAIL("non-unique key");
        }
    }

    V& InsertIfAbsent(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        return bucket.Map.insert(std::make_pair(key, value)).first->second;
    }

    template <typename Callable>
    V& InsertIfAbsentWithInit(const K& key, Callable initFunc) {
        TBucket& bucket = GetBucketForKey(key);
        TGuard<TLock> guard(bucket.Mutex);
        if (bucket.HasUnsafe(key)) {
            return bucket.GetUnsafe(key);
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
        if (bucket.HasUnsafe(key)) {
            result = bucket.GetUnsafe(key);
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
