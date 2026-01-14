#pragma once

#include <ydb/core/tx/datashard/datashard.h>

#include <library/cpp/cache/cache.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/rwlock.h>

#include <atomic>

namespace NKikimr::NKqp {

// Cached level table data - stores rows in a format ready for zero-copy access
// Uses shared_ptr for safe concurrent access without copying
struct TCachedLevelTableData : public TThrRefBase {
    // Store serialized cell vectors directly - no deserialization needed
    TVector<TString> SerializedRows;

    // Original record metadata (without cell data)
    NKikimrTxDataShard::TEvReadResult RecordMetadata;

    TCachedLevelTableData() = default;
};

using TCachedLevelTableDataPtr = TIntrusivePtr<TCachedLevelTableData>;

// Cache key - pre-computed hash for efficiency
struct TLevelTableCacheKey {
    TString TablePath;
    TString SerializedKeys;
    mutable size_t CachedHash = 0;

    bool operator==(const TLevelTableCacheKey& other) const {
        return TablePath == other.TablePath && SerializedKeys == other.SerializedKeys;
    }

    size_t Hash() const {
        if (CachedHash == 0) {
            CachedHash = CombineHashes(THash<TString>()(TablePath), THash<TString>()(SerializedKeys));
            if (CachedHash == 0) CachedHash = 1;  // Avoid recomputation
        }
        return CachedHash;
    }
};

} // namespace NKikimr::NKqp

template <>
struct THash<NKikimr::NKqp::TLevelTableCacheKey> {
    size_t operator()(const NKikimr::NKqp::TLevelTableCacheKey& key) const {
        return key.Hash();
    }
};

namespace NKikimr::NKqp {

// LRU cache entry wrapper
struct TLevelTableCacheEntry {
    TCachedLevelTableDataPtr Data;

    TLevelTableCacheEntry() = default;
    explicit TLevelTableCacheEntry(TCachedLevelTableDataPtr data)
        : Data(std::move(data))
    {}
};

// Global cache for vector index level table data
// Optimizations:
// - RWSpinLock: Multiple readers can access concurrently
// - LRU eviction: O(1) eviction instead of O(n)
// - Zero-copy: Returns shared pointer, no data copying on hit
// - Pre-computed hash: Avoids rehashing on every lookup
class TVectorIndexLevelCache {
public:
    static TVectorIndexLevelCache& Instance() {
        static TVectorIndexLevelCache instance;
        return instance;
    }

    // Try to get cached data - returns shared pointer (zero-copy)
    TCachedLevelTableDataPtr Get(const TString& tablePath, const TString& serializedKeys) {
        TReadGuard guard(Lock_);

        TLevelTableCacheKey key{tablePath, serializedKeys, 0};
        auto it = Cache_.Find(key);
        if (it != Cache_.End()) {
            return it.Value().Data;
        }
        return nullptr;
    }

    // Store data in cache
    void Put(const TString& tablePath, const TString& serializedKeys, TCachedLevelTableDataPtr data) {
        TWriteGuard guard(Lock_);

        TLevelTableCacheKey key{tablePath, serializedKeys, 0};
        Cache_.Insert(key, TLevelTableCacheEntry(std::move(data)));
    }

    // Clear all cache entries (for testing)
    void Clear() {
        TWriteGuard guard(Lock_);
        Cache_.Clear();
    }

    // Statistics
    size_t Size() const {
        TReadGuard guard(Lock_);
        return Cache_.Size();
    }

private:
    TVectorIndexLevelCache()
        : Cache_(MaxCacheEntries_)
    {}

    // Level tables are immutable, so cache can be long-lived
    static constexpr size_t MaxCacheEntries_ = 10000;

    mutable TRWMutex Lock_;
    TLRUCache<TLevelTableCacheKey, TLevelTableCacheEntry> Cache_;
};

} // namespace NKikimr::NKqp
