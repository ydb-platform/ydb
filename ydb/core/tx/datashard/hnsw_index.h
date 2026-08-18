#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>

#include <memory>
#include <atomic>
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
    void SetLimit(ui64 limit) noexcept {
        Limit.store(limit, std::memory_order_release);
    }

    ui64 GetLimit() const noexcept {
        return Limit.load(std::memory_order_acquire);
    }

    ui64 GetUsed() const noexcept {
        return Used.load(std::memory_order_acquire);
    }

    bool TryAcquire(ui64 bytes) noexcept {
        ui64 used = Used.load(std::memory_order_relaxed);
        while (true) {
            const ui64 limit = Limit.load(std::memory_order_acquire);
            if (!limit || used > limit || bytes > limit - used) {
                return false;
            }
            if (Used.compare_exchange_weak(used, used + bytes,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    void Release(ui64 bytes) noexcept {
        ui64 used = Used.load(std::memory_order_relaxed);
        while (!Used.compare_exchange_weak(used, bytes < used ? used - bytes : 0,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
        }
    }

private:
    std::atomic<ui64> Limit = 0;
    std::atomic<ui64> Used = 0;
};

// Result of an HNSW search: pairs of (serialized primary key, distance).
struct THnswSearchResult {
    std::vector<std::pair<TString, float>> Results;
};

// In-memory HNSW index over a single Float vector column, backed by
// ydb/library/nmslib. Immutable once built: to reflect new data, build a new
// instance and swap it in.
class THnswIndex {
public:
    ~THnswIndex();

    THnswIndex(const THnswIndex&) = delete;
    THnswIndex& operator=(const THnswIndex&) = delete;

    // Builds an index from (serialized primary key, raw vector bytes) pairs.
    // Vector bytes are in the KNN UDF wire format: elements followed by a
    // trailing 1-byte format tag; only FloatVector is supported here.
    // Returns nullptr and sets `error` if the settings/data are not eligible
    // (e.g. non-float vector type, empty input, invalid vector bytes) or if
    // the estimated memory to hold the index would exceed maxMemoryBytes.
    static std::unique_ptr<THnswIndex> Build(
        const Ydb::Table::VectorIndexSettings& settings,
        const std::vector<std::pair<TString, TString>>& keysAndVectors,
        ui64 maxMemoryBytes,
        TString& error);

    // Returns up to k nearest neighbors of targetVector (same wire format as
    // build-time vectors), ordered from closest to farthest.
    THnswSearchResult Search(TStringBuf targetVector, size_t k) const;

    // Copies the original wire-format vector for a key into `result`. This is
    // used by covered posting-table reads to materialize HNSW results without
    // going back to the flat table.
    bool GetVector(TStringBuf key, TString& result) const;

    // Applies a posting-table change on top of the immutable HNSW graph.
    // Updated vectors are searched exhaustively and shadow the graph entry;
    // erased keys are filtered from graph results.
    bool Upsert(TString key, TString vector);
    void Erase(TStringBuf key);
    bool HasDelta(TStringBuf key) const;
    bool HasChanges() const;
    size_t ChangeCount() const;

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
