#pragma once

#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <library/cpp/cache/cache.h>
#include <util/system/spinlock.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <memory>

namespace NKikimr {
namespace NKqp {

/**
 * Represents cached cluster data for a specific level in a vector index.
 * Stores raw cluster data that can be used to recreate IClusters objects.
 */
struct TCachedClusterLevel {
    TVector<ui64> ClusterIds;
    TVector<TString> ClusterData; // Raw centroid data
    TInstant CachedAt;
    
    // Index settings needed to recreate clusters
    Ydb::Table::VectorIndexSettings IndexSettings;

    TCachedClusterLevel() = default;
    
    TCachedClusterLevel(TVector<ui64> clusterIds, 
                       TVector<TString> clusterData,
                       const Ydb::Table::VectorIndexSettings& indexSettings)
        : ClusterIds(std::move(clusterIds))
        , ClusterData(std::move(clusterData))
        , CachedAt(TInstant::Now())
        , IndexSettings(indexSettings)
    {
    }

    /**
     * Create an IClusters object from the cached data.
     */
    std::unique_ptr<NKikimr::NKMeans::IClusters> CreateClusters(TString& error) const;
};

/**
 * Key for identifying cached level table data.
 * Uses table path and schema version to ensure cache validity.
 */
struct TLevelTableCacheKey {
    TString TablePath;
    ui64 SchemaVersion;
    ui64 ParentCluster; // 0 for root level

    TLevelTableCacheKey(const TString& tablePath, ui64 schemaVersion, ui64 parentCluster = 0)
        : TablePath(tablePath)
        , SchemaVersion(schemaVersion)
        , ParentCluster(parentCluster)
    {
    }

    bool operator==(const TLevelTableCacheKey& other) const {
        return TablePath == other.TablePath && 
               SchemaVersion == other.SchemaVersion && 
               ParentCluster == other.ParentCluster;
    }

    TString ToString() const {
        return TStringBuilder() << TablePath << ":" << SchemaVersion << ":" << ParentCluster;
    }
};

/**
 * Cache for vector index level table data.
 * Stores cluster hierarchies to avoid repeated reads from level tables.
 */
class TKqpVectorLevelCache {
public:
    using TCacheKey = TLevelTableCacheKey;
    using TCacheValue = TCachedClusterLevel;

    explicit TKqpVectorLevelCache(size_t maxSize = 1000, TDuration ttl = TDuration::Hours(1))
        : MaxSize(maxSize)
        , Ttl(ttl)
    {
    }

    /**
     * Try to get cached cluster data for the specified key.
     * Returns nullptr if not found or expired.
     */
    std::shared_ptr<TCacheValue> Get(const TCacheKey& key);

    /**
     * Store cluster data in cache.
     */
    void Put(const TCacheKey& key, std::shared_ptr<TCacheValue> value);

    /**
     * Invalidate all cache entries for a specific table.
     * Should be called when the vector index is rebuilt.
     */
    void InvalidateTable(const TString& tablePath);

    /**
     * Clear all cached data.
     */
    void Clear();

    /**
     * Get cache statistics.
     */
    struct TStats {
        size_t Size;
        size_t Hits;
        size_t Misses;
    };
    TStats GetStats() const;

private:
    void CleanupExpired();
    
    struct TCacheEntry {
        std::shared_ptr<TCacheValue> Value;
        TInstant ExpiredAt;
        
        TCacheEntry(std::shared_ptr<TCacheValue> value, TInstant expiredAt)
            : Value(std::move(value))
            , ExpiredAt(expiredAt)
        {
        }
    };

    mutable TAdaptiveLock Lock;
    THashMap<TString, TCacheEntry> Cache; // Key is TCacheKey.ToString()
    
    size_t MaxSize;
    TDuration Ttl;
    
    // Statistics
    mutable size_t Hits = 0;
    mutable size_t Misses = 0;
};

} // namespace NKqp
} // namespace NKikimr

// Hash specialization for TLevelTableCacheKey
template <>
struct THash<NKikimr::NKqp::TLevelTableCacheKey> {
    size_t operator()(const NKikimr::NKqp::TLevelTableCacheKey& key) const {
        return CombineHashes(
            THash<TString>()(key.TablePath),
            CombineHashes(
                THash<ui64>()(key.SchemaVersion),
                THash<ui64>()(key.ParentCluster)
            )
        );
    }
};