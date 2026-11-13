#pragma once

#include "data_stats_by_storage.h"

#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

#include <ydb/library/accessor/positive_integer.h>

#include <util/generic/refcount.h>

#include <map>
#include <optional>

namespace NKikimrTableStats {
class TTableStats;
}

namespace NKikimr::NOlap {
class IStoragesManager;
}

namespace NKikimr::NColumnShard {

class TTabletCountersHandle;

// Every number the shard reports about its portions; the granule hooks are the only way in
class TShardDataStats {
private:
    const std::shared_ptr<NOlap::IStoragesManager> StoragesManager;
    const ui64 TabletId;
    const TEngineLogsCounters Counters;

    THashMap<TInternalPathId, TDataStatsByStorage> Granules;
    TDataStatsByStorage Total;
    THashMap<TString, ui64> SmallBlobThresholds;
    // RAM taken by the metadata of the shard's portions; writes are rejected when the sum over all shards reaches a limit
    i64 PortionsMetadataRamBytes = 0;
    static inline TAtomicCounter AllShardsPortionsMetadataRamBytes = 0;
    // schema version -> portions written with it
    std::map<ui64, TPositiveControlInteger> SchemaVersionsInUse;
    TEngineLogsCounters::TPortionSizeHistograms PortionSizeHistograms;

    // We assume that all the blob storage groups for a database have the same layout (erasure, etc.)
    std::optional<TBlobStorageGroupType> BlobStorageLayout;

    const std::optional<TBlobStorageGroupType>& GetBlobStorageLayout();
    ui64 NormalizeSmallBlobsCount(const ui64 rawCount);
    ui64 GetSmallBlobThreshold(const TString& storageId);

public:
    TShardDataStats(const std::shared_ptr<NOlap::IStoragesManager>& storagesManager, const ui64 tabletId);
    ~TShardDataStats();

    void AddPortion(const std::shared_ptr<NOlap::TPortionInfo>& portion);
    void RemovePortion(const std::shared_ptr<NOlap::TPortionInfo>& portion);

    const TDataStatsByStorage& GetGranuleStats(const TInternalPathId pathId) const;

    const TStorageStats& GetStorageStats(const TString& storageId) const {
        return Total.GetStorageStats(storageId);
    }

    bool HasSchemaVersion(const ui64 fromVersion, const ui64 version) const;

    static ui64 GetAllShardsPortionsMetadataRamBytes() {
        return AllShardsPortionsMetadataRamBytes.Val();
    }

    void FillTableStats(const TInternalPathId pathId, NKikimrTableStats::TTableStats& tableStats) const;
    // also counts the small blobs waiting for deletion
    void FillTotalTableStats(NKikimrTableStats::TTableStats& tableStats);
    void UpdateTabletCounters(const TTabletCountersHandle& counters) const;
    ui64 GetBlobStorageBytes() const;
};

}   // namespace NKikimr::NColumnShard
