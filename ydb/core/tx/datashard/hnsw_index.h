#pragma once

#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/row_version.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>

#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

// Proto3 optional fields have no schema-level defaults. Keep the documented
// server-side default in one place so omitted SQL settings do not behave as 0.
ui64 GetHnswMinRows(const Ydb::Table::VectorIndexSettings& settings);

// Returns whether an index built with `cached` may serve a request with
// `requested`. This compares normalized values, so an omitted HNSW parameter
// and the corresponding explicit default are treated identically.
bool AreHnswIndexSettingsCompatible(
    const Ydb::Table::VectorIndexSettings& cached,
    const Ydb::Table::VectorIndexSettings& requested);

class THnswCacheMemoryTracker {
public:
    void SetConsumer(TIntrusivePtr<NMemory::IMemoryConsumer> consumer) {
        std::lock_guard guard(Mutex);
        Consumer = std::move(consumer);
        Report();
    }

    void SetLimit(ui64 limit) noexcept {
        std::lock_guard guard(Mutex);
        Limit = limit;
        LimitKnown = true;
    }

    bool HasLimit() const noexcept {
        std::lock_guard guard(Mutex);
        return LimitKnown;
    }

    ui64 GetLimit() const noexcept {
        std::lock_guard guard(Mutex);
        return Limit;
    }

    ui64 GetUsed() const noexcept {
        std::lock_guard guard(Mutex);
        return Used;
    }

    bool TryAcquire(ui64 bytes) noexcept {
        std::lock_guard guard(Mutex);
        if (bytes > Max<ui64>() - Used) {
            return false;
        }
        // A graph must fit as a whole. Report failed reservations as demand so
        // the controller can grow our share beyond the bootstrap allowance.
        if (!Limit || Used > Limit || bytes > Limit - Used) {
            PendingDemand = Max(PendingDemand, Used + bytes);
            Report();
            return false;
        }
        Used += bytes;
        Report();
        return true;
    }

    void Release(ui64 bytes) noexcept {
        std::lock_guard guard(Mutex);
        const ui64 released = Min(bytes, Used);
        Used -= released;
        Report();
    }

    void ResetDemand() {
        std::lock_guard guard(Mutex);
        PendingDemand = 0;
        Report();
    }

private:
    void Report() {
        if (Consumer) {
            // Serialize reports with reservations: a release on a build/reader
            // thread must not overwrite a newer report with stale usage.
            Consumer->SetReport({.Used = Used, .Demand = Max(Used, PendingDemand)});
        }
    }

    mutable std::mutex Mutex;
    ui64 Limit = 0;
    bool LimitKnown = false;
    ui64 Used = 0;
    // Preserve the whole request when an unsuccessful build releases its
    // partial reservations. Cleared on completion, invalidation, or eviction.
    ui64 PendingDemand = 0;
    TIntrusivePtr<NMemory::IMemoryConsumer> Consumer;
};

// Result of an HNSW search: pairs of (serialized primary key, distance).
struct THnswSearchResult {
    std::vector<std::pair<TString, float>> Results;
    bool Covered = true;
};

// Owned by the tablet mailbox. Generations share committed history, never
// transaction-local intents. Build workers only access the immutable graph.
class THnswIndexChanges {
public:
    struct TVersion {
        TRowVersion Version;
        std::optional<TString> Vector;
        std::shared_ptr<void> MemoryReservation;
    };
    using THistory = std::vector<TVersion>;

    void Set(TString key, TRowVersion version, std::optional<TString> vector,
        std::shared_ptr<void> reservation = {});
    static const TVersion* Find(const THistory& history, TRowVersion version, TRowVersion base);
    const TVersion* Find(TStringBuf key, TRowVersion version, TRowVersion base) const;
    size_t CountAfter(TRowVersion base) const;
    // Every remaining generation already contains these versions in its base.
    void PruneThrough(TRowVersion base);
    static size_t EstimateBytes(TStringBuf key, size_t vectorBytes);

    size_t GetVersionCount() const { return VersionCount; }
    size_t GetEstimatedBytes() const { return EstimatedBytes; }

    THashMap<TString, THistory> Rows;
    bool Valid = true;

private:
    // A write updates one counter per live base, rather than scanning the
    // entire overlay to decide whether an asynchronous rebuild is needed.
    mutable TMap<TRowVersion, size_t> ChangedRowCounts;
    size_t VersionCount = 0;
    size_t EstimatedBytes = 0;
};

// In-memory HNSW index over a single Float vector column, backed by
// ydb/library/nmslib. The graph is immutable; committed changes live in a
// versioned journal shared by the tablet's retained graph generations.
class THnswIndex {
public:
    ~THnswIndex();

    THnswIndex(const THnswIndex&) = delete;
    THnswIndex& operator=(const THnswIndex&) = delete;

    // Builds an index from (serialized primary key, raw vector bytes) pairs.
    // Vector bytes are in the KNN UDF wire format: elements followed by a
    // trailing 1-byte format tag; only FloatVector is supported here.
    // Returns nullptr and sets `error` if the settings/data are not eligible
    // (e.g. non-float vector type, invalid vector bytes, empty input unless
    // allowEmpty is set for a rebuild) or if
    // the estimated memory to hold the index would exceed maxMemoryBytes.
    static std::unique_ptr<THnswIndex> Build(
        const Ydb::Table::VectorIndexSettings& settings,
        const std::vector<std::pair<TString, TString>>& keysAndVectors,
        ui64 maxMemoryBytes,
        TString& error, bool allowEmpty = false);

    // Returns up to k nearest neighbors of targetVector (same wire format as
    // build-time vectors), ordered from closest to farthest.
    THnswSearchResult Search(TStringBuf targetVector, size_t k,
        TRowVersion readVersion = TRowVersion::Max()) const;

    // Resolves the visible vector; base vectors are reconstructed from the
    // wrapper-owned NMSLIB Objects, and delta vectors retain their wire format.
    bool GetVector(TStringBuf key, TString& result,
        TRowVersion readVersion = TRowVersion::Max()) const;

    // Applies a posting-table change on top of the immutable HNSW graph.
    // Updated vectors are searched exhaustively and shadow the graph entry;
    // erased keys are filtered from graph results.
    bool Upsert(TString key, TString vector, TRowVersion version = TRowVersion::Max());
    void Erase(TStringBuf key, TRowVersion version = TRowVersion::Max());
    bool HasDelta(TStringBuf key) const;
    bool HasChanges() const;
    size_t ChangeCount() const;

    void SetSnapshot(TRowVersion base, std::shared_ptr<THnswIndexChanges> changes,
        TRowVersion upper = TRowVersion::Max());
    TRowVersion GetBaseVersion() const;
    bool CanRead(TRowVersion version) const;
    bool NeedsRebuild(ui32 thresholdPercent) const;
    static bool IsValidVector(TStringBuf vector, size_t dimension);

    size_t Size() const;
    size_t Dimension() const;

    // Estimated resident memory of this index, in bytes. Computed once at
    // build time from the same formula used to gate the build.
    size_t EstimatedMemoryBytes() const;

    // Estimates the memory required to hold an HNSW index over `rowCount`
    // vectors of `dimension` float elements, without building anything.
    // Used to decide up front whether a build should even be attempted.
    static size_t EstimateMemoryBytes(size_t rowCount, size_t dimension, ui32 connectivity = 16,
        size_t serializedKeyBytes = 0);

private:
    class TImpl;

    explicit THnswIndex(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl;
};

} // namespace NKikimr::NDataShard
