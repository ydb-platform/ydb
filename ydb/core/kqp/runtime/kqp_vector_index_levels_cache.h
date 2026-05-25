#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <library/cpp/cache/cache.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/rwlock.h>

#include <atomic>

namespace NKikimr::NKqp {

// Cached level table data - stores rows in a format ready for zero-copy access
// Uses shared_ptr for safe concurrent access without copying
struct TCachedLevelTableData : public TAtomicRefCount<TCachedLevelTableData> {
    // Store serialized cell vectors directly - no deserialization needed
    TOwnedCellVecBatch BatchRows;

    TCachedLevelTableData() = default;
};

using TCachedLevelTableDataPtr = TIntrusivePtr<TCachedLevelTableData>;

struct TLevelTableCacheKey {
    TPathId TableId;
    TString SerializedKeys;

    bool operator==(const TLevelTableCacheKey& other) const {
        return TableId == other.TableId && SerializedKeys == other.SerializedKeys;
    }

    size_t Hash() const {
        return CombineHashes(THash<TPathId>()(TableId), THash<TString>()(SerializedKeys));
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
    size_t ByteSize = 0;  // approximate memory footprint, computed once at insertion

    TLevelTableCacheEntry() = default;
    TLevelTableCacheEntry(TCachedLevelTableDataPtr data, size_t byteSize)
        : Data(std::move(data))
        , ByteSize(byteSize)
    {}
};

// Approximate memory footprint of cached data. Used both to drive the bytes
// gauge and to give operators a sense of cache pressure.
inline size_t EstimateCachedDataBytes(const TCachedLevelTableData& data) {
    return data.BatchRows.DataSizeEstimate();
}

class TVectorIndexLevelsCache : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TVectorIndexLevelsCache>;

public:

    // Try to get cached data - returns shared pointer (zero-copy)
    TCachedLevelTableDataPtr Get(const TPathId& tableId, const TString& serializedKeys) {
        TWriteGuard guard(Lock_);

        TLevelTableCacheKey key{tableId, serializedKeys};
        auto it = Cache_.Find(key);
        if (it != Cache_.End()) {
            Hits_->Inc();
            return it.Value().Data;
        }
        Misses_->Inc();
        return nullptr;
    }

    // Store data in cache
    void Put(const TPathId& tableId, const TString& serializedKeys, TCachedLevelTableDataPtr data) {
        const size_t bytes = data ? EstimateCachedDataBytes(*data) : 0;

        TWriteGuard guard(Lock_);

        TLevelTableCacheKey key{tableId, serializedKeys};

        if (auto existing = Cache_.FindWithoutPromote(key); existing != Cache_.End()) {
            return;
        } else {
            Evict(bytes);
            Cache_.Insert(key, TLevelTableCacheEntry(std::move(data), bytes));
            BytesTotal_ += bytes;
            Inserts_->Inc();
            RefreshGauges();
        }
    }

    void Evict(ui64 bytesToAdd = 0) {
        // Byte-cap eviction. The TLRUCache only caps by entry count; with
        // multi-KB rows (embedding columns) one entry can be megabytes, so
        // the entry-count cap alone can let total bytes balloon.
        auto maxBytes = MaxCacheBytes_.load();
        while (BytesTotal_ + bytesToAdd > maxBytes && Cache_.Size() >= 1) {
            auto oldest = Cache_.FindOldest();
            Y_ENSURE(oldest != Cache_.End());
            BytesTotal_ -= oldest.Value().ByteSize;
            Cache_.Erase(oldest);
            Invalidations_->Inc();
        }
    }

    // Number of cached entries. Cheap; primarily useful for tests.
    size_t Size() const {
        TReadGuard guard(Lock_);
        return Cache_.Size();
    }

    size_t MaxBytes() const { return MaxCacheBytes_.load(); }

    void SetMaxBytes(ui64 maxBytes) {
        MaxCacheBytes_.store(maxBytes);

        TWriteGuard guard(Lock_);

        Evict();

        RefreshGauges();
    }

    // Counter getters for monitoring (cumulative since process start).
    ui64 HitsTotal() const { return Hits_ ? Hits_->Val() : 0; }
    ui64 MissesTotal() const { return Misses_ ? Misses_->Val() : 0; }
    ui64 InsertsTotal() const { return Inserts_ ? Inserts_->Val() : 0; }
    ui64 InvalidationsTotal() const { return Invalidations_ ? Invalidations_->Val() : 0; }

    // Approximate bytes currently held in the cache. O(1).
    size_t Bytes() const {
        TReadGuard guard(Lock_);
        return BytesTotal_;
    }

    // Counters argument: a parent group under which the cache exposes its
    // metrics. Pass nullptr in tests / contexts without a counters tree — the
    // cache attaches to a private group and metric updates become no-op-ish.
    // maxCacheBytes caps total cached bytes (rows + metadata)
    explicit TVectorIndexLevelsCache(::NMonitoring::TDynamicCounterPtr counters = nullptr)
        : MaxCacheBytes_(0)
        , Cache_(MaxCacheEntries_)
    {
        auto group = counters
            ? counters->GetSubgroup("subsystem", "VectorIndexLevelsCache")
            : MakeIntrusive<::NMonitoring::TDynamicCounters>();

        MaxSizeGauge_ = group->GetCounter("MaxSize", false);
        BytesGauge_ = group->GetCounter("Bytes", false);
        EntriesGauge_ = group->GetCounter("Entries", false);
        Hits_ = group->GetCounter("Hits", true);
        Misses_ = group->GetCounter("Misses", true);
        Inserts_ = group->GetCounter("Inserts", true);
        Invalidations_ = group->GetCounter("Invalidations", true);
    }

private:
    // Level tables are immutable, so cache can be long-lived. Entry-count
    // cap is a coarse upper bound; the byte cap below is the active limit.
    static constexpr size_t MaxCacheEntries_ = 10000;

    // Must be called with the write lock held.
    void RefreshGauges() {
        BytesGauge_->Set(BytesTotal_);
        EntriesGauge_->Set(Cache_.Size());
        MaxSizeGauge_->Set(MaxCacheBytes_.load());
    }

    std::atomic<ui64> MaxCacheBytes_;
    size_t BytesTotal_ = 0;

    mutable TRWMutex Lock_;
    TLRUCache<TLevelTableCacheKey, TLevelTableCacheEntry> Cache_;

    ::NMonitoring::TDynamicCounters::TCounterPtr MaxSizeGauge_;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesGauge_;
    ::NMonitoring::TDynamicCounters::TCounterPtr EntriesGauge_;
    ::NMonitoring::TDynamicCounters::TCounterPtr Hits_;
    ::NMonitoring::TDynamicCounters::TCounterPtr Misses_;
    ::NMonitoring::TDynamicCounters::TCounterPtr Inserts_;
    ::NMonitoring::TDynamicCounters::TCounterPtr Invalidations_;
};

IActor* CreateVectorIndexLevelsCacheMaintainer(
    TIntrusivePtr<TVectorIndexLevelsCache> cache,
    std::shared_ptr<NRm::IKqpResourceManager> rm,
    const NKikimrConfig::TTableServiceConfig::TResourceManager& initialConfig);

} // namespace NKikimr::NKqp