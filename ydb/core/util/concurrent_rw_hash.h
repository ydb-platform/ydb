#pragma once

#include <util/generic/hash.h>
#include <util/system/rwlock.h>

#include <array>

template <typename K, typename V, size_t BucketCount = 64>
class TConcurrentRWHashMap {
public:
    using TActualMap = THashMap<K, V>;

    struct TBucket {
        friend class TConcurrentRWHashMap;

    private:
        TActualMap Map;
        mutable TRWMutex RWLock;

    public:
        TRWMutex& GetLock() const {
            return RWLock;
        }

        TActualMap& GetMap() {
            return Map;
        }

        const TActualMap& GetMap() const {
            return Map;
        }
    };

    std::array<TBucket, BucketCount> Buckets;

public:
    Y_FORCE_INLINE TBucket& GetBucketForKey(const K& key) {
        return Buckets[THash<K>()(key) % BucketCount];
    }

    Y_FORCE_INLINE const TBucket& GetBucketForKey(const K& key) const {
        return Buckets[THash<K>()(key) % BucketCount];
    }

    void Insert(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        bucket.Map[key] = value;
    }

    bool Swap(const K& key, const V& value, V& out_prev_value) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        typename TActualMap::iterator it = bucket.Map.find(key);
        if (it != bucket.Map.end()) {
            out_prev_value = it->second;
            it->second = value;
            return true;
        }
        bucket.Map.insert(std::make_pair(key, value));
        return false;
    }

    V& InsertIfAbsent(const K& key, const V& value) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        return bucket.Map.insert(std::make_pair(key, value)).first->second;
    }

    template <typename Callable>
    V& InsertIfAbsentWithInit(const K& key, Callable initFunc) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        typename TActualMap::iterator it = bucket.Map.find(key);
        if (it != bucket.Map.end()) {
            return it->second;
        }
        return bucket.Map.insert(std::make_pair(key, initFunc())).first->second;
    }

    V Get(const K& key) const {
        const TBucket& bucket = GetBucketForKey(key);
        TReadGuard guard(bucket.RWLock);

        typename TActualMap::const_iterator it = bucket.Map.find(key);
        Y_ABORT_UNLESS(it != bucket.Map.end(), "not found by key");
        return it->second;
    }

    bool Get(const K& key, V& result) const {
        const TBucket& bucket = GetBucketForKey(key);
        TReadGuard guard(bucket.RWLock);

        typename TActualMap::const_iterator it = bucket.Map.find(key);
        if (it != bucket.Map.end()) {
            result = it->second;
            return true;
        }
        return false;
    }

    V Remove(const K& key) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        typename TActualMap::iterator it = bucket.Map.find(key);
        Y_ABORT_UNLESS(it != bucket.Map.end(), "removing non-existent key");
        V r = it->second;
        bucket.Map.erase(it);
        return r;
    }

    bool Has(const K& key) const {
        const TBucket& bucket = GetBucketForKey(key);
        TReadGuard guard(bucket.RWLock);

        typename TActualMap::const_iterator it = bucket.Map.find(key);
        return (it != bucket.Map.end());
    }

    bool Erase(const K& key) {
        TBucket& bucket = GetBucketForKey(key);
        TWriteGuard guard(bucket.RWLock);

        return bucket.Map.erase(key);
    }
};
