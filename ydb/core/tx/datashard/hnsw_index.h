#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>

#include <memory>
#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

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

    size_t Size() const;
    size_t Dimension() const;

    // Estimated resident memory of this index, in bytes. Computed once at
    // build time from the same formula used to gate the build.
    size_t EstimatedMemoryBytes() const;

    // Estimates the memory required to hold an HNSW index over `rowCount`
    // vectors of `dimension` float elements, without building anything.
    // Used to decide up front whether a build should even be attempted.
    static size_t EstimateMemoryBytes(size_t rowCount, size_t dimension);

private:
    class TImpl;

    explicit THnswIndex(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl;
};

} // namespace NKikimr::NDataShard
