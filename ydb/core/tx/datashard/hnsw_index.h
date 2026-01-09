#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NKikimr::NDataShard {

// Forward declaration of the implementation
class THnswIndexImpl;

// Result of HNSW search - pairs of (key, distance)
struct THnswSearchResult {
    std::vector<std::pair<ui64, float>> Results;
};

// HNSW index wrapper for a single vector column
// Currently supports only Float vectors with CosineDistance metric
class THnswIndex {
public:
    THnswIndex();
    ~THnswIndex();

    THnswIndex(THnswIndex&&) noexcept;
    THnswIndex& operator=(THnswIndex&&) noexcept;

    // Build index from data
    // vectors: vector of (row key, float vector data) pairs (serialized as string)
    bool Build(const std::vector<std::pair<ui64, TString>>& vectors);

    // Search for top-K nearest neighbors
    // targetVector: serialized float vector
    // k: number of results to return
    THnswSearchResult Search(const TString& targetVector, size_t k) const;

    // Check if index is ready for search
    bool IsReady() const;

    // Get number of vectors in index
    size_t Size() const;

private:
    std::unique_ptr<THnswIndexImpl> Impl;
};

// Key for identifying an HNSW index: (tableId, columnName)
struct THnswIndexKey {
    ui64 TableId;
    TString ColumnName;

    bool operator==(const THnswIndexKey& other) const {
        return TableId == other.TableId && ColumnName == other.ColumnName;
    }
};

struct THnswIndexKeyHash {
    size_t operator()(const THnswIndexKey& key) const {
        return CombineHashes(THash<ui64>()(key.TableId), THash<TString>()(key.ColumnName));
    }
};

// Manager class holding HNSW indexes for all vector columns in a datashard
class THnswIndexManager {
public:
    THnswIndexManager() = default;

    // Check if column should have HNSW index (name is emb, embedding, or vector)
    static bool IsVectorColumn(const TString& columnName);

    // Build index for a specific table/column
    // Returns true if build succeeded
    bool BuildIndex(ui64 tableId, const TString& columnName,
                    const std::vector<std::pair<ui64, TString>>& vectors);

    // Get index for search (returns nullptr if not available)
    const THnswIndex* GetIndex(ui64 tableId, const TString& columnName) const;

    // Check if index exists and is ready
    bool HasIndex(ui64 tableId, const TString& columnName) const;

    // Clear all indexes
    void Clear();

private:
    THashMap<THnswIndexKey, THnswIndex, THnswIndexKeyHash> Indexes;
};

} // namespace NKikimr::NDataShard



