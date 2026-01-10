#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <memory>
#include <mutex>
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

    // Get embedding vectors for multiple keys (batch)
    // Returns a vector of serialized embeddings (Float format), empty string for not found keys
    std::vector<TString> GetVectors(const std::vector<ui64>& keys) const;

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

// Key for node-level shared HNSW index cache: (tabletId, tableId, columnName)
// TabletId is shared between leader and all followers of the same logical tablet
struct TNodeHnswIndexKey {
    ui64 TabletId;
    ui64 TableId;
    TString ColumnName;

    bool operator==(const TNodeHnswIndexKey& other) const {
        return TabletId == other.TabletId && TableId == other.TableId && ColumnName == other.ColumnName;
    }
};

struct TNodeHnswIndexKeyHash {
    size_t operator()(const TNodeHnswIndexKey& key) const {
        return CombineHashes(
            CombineHashes(THash<ui64>()(key.TabletId), THash<ui64>()(key.TableId)),
            THash<TString>()(key.ColumnName));
    }
};

// Node-level shared cache for HNSW indexes
// Leader builds the index and registers it here; followers reuse it instead of rebuilding
// Thread-safe singleton accessed by all datashards on the same node
// Includes protection against concurrent builds (only one builder per index key)
class TNodeHnswIndexCache {
public:
    static TNodeHnswIndexCache& Instance();

    // Register an index built by the leader
    // Returns shared_ptr to the index (caller should store it)
    std::shared_ptr<THnswIndex> RegisterIndex(ui64 tabletId, ui64 tableId, const TString& columnName,
                                               std::unique_ptr<THnswIndex> index);

    // Get existing index from cache (for leader restart or followers)
    // Returns nullptr if not found or index size doesn't match expectedSize (if provided)
    // expectedSize=0 means don't check size (use for followers that don't know expected size)
    std::shared_ptr<THnswIndex> GetIndex(ui64 tabletId, ui64 tableId, const TString& columnName,
                                          size_t expectedSize = 0) const;

    // Check if index exists in cache with matching size
    bool HasIndex(ui64 tabletId, ui64 tableId, const TString& columnName, size_t expectedSize = 0) const;

    // Check if a build is currently in progress for this index
    bool IsBuildInProgress(ui64 tabletId, ui64 tableId, const TString& columnName) const;

    // Mark that a build is starting for this index (prevents concurrent builds)
    // Returns true if this caller should build, false if another build is in progress
    bool TryStartBuild(ui64 tabletId, ui64 tableId, const TString& columnName);

    // Mark that build finished (called after RegisterIndex or on build failure)
    void FinishBuild(ui64 tabletId, ui64 tableId, const TString& columnName);

    // Remove index from cache (when tablet is destroyed)
    void RemoveIndex(ui64 tabletId, ui64 tableId, const TString& columnName);

    // Remove all indexes for a tablet (when tablet is destroyed)
    void RemoveTabletIndexes(ui64 tabletId);

    // Global cache statistics (survives tablet restarts on same node)
    ui64 GetCacheHits() const;
    ui64 GetCacheMisses() const;
    void IncrementCacheHit();
    void IncrementCacheMiss();

private:
    TNodeHnswIndexCache() = default;

    mutable std::mutex Mutex;
    THashMap<TNodeHnswIndexKey, std::shared_ptr<THnswIndex>, TNodeHnswIndexKeyHash> Indexes;
    THashSet<TNodeHnswIndexKey, TNodeHnswIndexKeyHash> BuildingIndexes;  // Indexes currently being built
    mutable std::atomic<ui64> GlobalCacheHits{0};
    mutable std::atomic<ui64> GlobalCacheMisses{0};
};

// Manager class holding HNSW indexes for all vector columns in a datashard
// For leaders: builds indexes and registers them in node cache
// For followers: retrieves indexes from node cache (built by leader on same node)
class THnswIndexManager {
public:
    THnswIndexManager() = default;

    // Set tablet ID for node-level cache lookup
    void SetTabletId(ui64 tabletId) { TabletId = tabletId; }

    // Statistics for debugging/testing
    ui64 GetCacheHits() const { return CacheHits; }
    ui64 GetCacheMisses() const { return CacheMisses; }
    void ResetStats() { CacheHits = 0; CacheMisses = 0; }

    // Get info about all indexes for debugging
    struct TIndexInfo {
        ui64 TableId;
        TString ColumnName;
        size_t IndexSize;
        bool IsReady;
        ui64 Reads = 0;
        ui64 FastPathReads = 0;
        ui64 SlowPathReads = 0;
        ui64 PageFaults = 0;
    };
    std::vector<TIndexInfo> GetAllIndexesInfo() const;

    // Increment statistics for a specific index
    void IncrementReads(ui64 tableId, const TString& columnName);
    void IncrementFastPathReads(ui64 tableId, const TString& columnName);
    void IncrementSlowPathReads(ui64 tableId, const TString& columnName);
    void IncrementPageFaults(ui64 tableId, const TString& columnName);

    // Check if column should have HNSW index (name is emb, embedding, or vector)
    static bool IsVectorColumn(const TString& columnName);

    // Build index for a specific table/column (for leader)
    // Registers the built index in node cache for sharing with followers
    // Returns true if build succeeded
    bool BuildIndex(ui64 tableId, const TString& columnName,
                    const std::vector<std::pair<ui64, TString>>& vectors);

    // Try to get index from node cache (for leader restart or followers)
    // expectedSize=0 means don't check size (use for followers that don't know expected size)
    // Returns true if found in cache with matching size, false if not
    bool TryGetFromCache(ui64 tableId, const TString& columnName, size_t expectedSize = 0);

    // Get index for search (returns nullptr if not available)
    const THnswIndex* GetIndex(ui64 tableId, const TString& columnName) const;

    // Check if index exists and is ready
    bool HasIndex(ui64 tableId, const TString& columnName) const;

    // Clear all indexes (local references only; node cache entries persist)
    void Clear();

    // Clear and remove from node cache (call when tablet is destroyed)
    void ClearAndRemoveFromCache();

private:
    ui64 TabletId = 0;
    // Store shared_ptr to support sharing with node cache
    THashMap<THnswIndexKey, std::shared_ptr<THnswIndex>, THnswIndexKeyHash> Indexes;
    // Statistics for debugging/testing
    mutable ui64 CacheHits = 0;
    mutable ui64 CacheMisses = 0;
    // Per-index read statistics
    struct TIndexStats {
        std::atomic<ui64> Reads{0};
        std::atomic<ui64> FastPathReads{0};
        std::atomic<ui64> SlowPathReads{0};
        std::atomic<ui64> PageFaults{0};
    };
    mutable THashMap<THnswIndexKey, TIndexStats, THnswIndexKeyHash> IndexStats;
    mutable std::mutex StatsMutex;
};

} // namespace NKikimr::NDataShard



