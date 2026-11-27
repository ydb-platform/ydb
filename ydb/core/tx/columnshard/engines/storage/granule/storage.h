#pragma once
#include "granule.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>

namespace NKikimr::NOlap {

class TGranuleOrdered {
private:
    NStorageOptimizer::TOptimizationPriority Priority;
    YDB_READONLY_DEF(std::shared_ptr<TGranuleMeta>, Granule);

public:
    const NStorageOptimizer::TOptimizationPriority& GetPriority() const {
        return Priority;
    }

    TGranuleOrdered(const NStorageOptimizer::TOptimizationPriority& priority, const std::shared_ptr<TGranuleMeta>& meta)
        : Priority(priority)
        , Granule(meta)
    {
    }

    bool operator<(const TGranuleOrdered& item) const {
        return Priority < item.Priority;
    }
};

class TGranulesStorage {
private:
    const NColumnShard::TEngineLogsCounters Counters;
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::shared_ptr<IStoragesManager> StoragesManager;
    THashMap<TInternalPathId, std::shared_ptr<TGranuleMeta>> Tables;   // pathId into Granule that equal to Table
    const std::shared_ptr<NColumnShard::TShardDataStats> DataStats;
    std::shared_ptr<NStorageOptimizer::TOptimizerRuntimeSettings> OptimizerRuntimeSettings =
        NStorageOptimizer::TOptimizerRuntimeSettings::MakeDefault();

public:
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& GetDataAccessorsManager() const {
        return DataAccessorsManager;
    }

    const std::shared_ptr<NStorageOptimizer::TOptimizerRuntimeSettings>& GetOptimizerRuntimeSettings() const {
        return OptimizerRuntimeSettings;
    }

    std::vector<TCSMetadataRequest> CollectMetadataRequests() {
        std::vector<TCSMetadataRequest> result;
        for (auto&& i : Tables) {
            auto r = i.second->CollectMetadataRequests();
            if (!r.size()) {
                continue;
            }
            result.insert(result.end(), r.begin(), r.end());
        }
        return result;
    }

    TGranulesStorage(const NColumnShard::TEngineLogsCounters counters,
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& dataAccessorsManager,
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<NColumnShard::TShardDataStats>& dataStats)
        : Counters(counters)
        , DataAccessorsManager(dataAccessorsManager)
        , StoragesManager(storagesManager)
        , DataStats(dataStats)
    {
        YDB_LOG_VERIFY(DataAccessorsManager);
        YDB_LOG_VERIFY(StoragesManager);
        YDB_LOG_VERIFY(DataStats);
    }

    void FetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const {
        DataAccessorsManager->AskData(request);
    }

    const std::shared_ptr<NColumnShard::TShardDataStats>& GetDataStats() const {
        return DataStats;
    }

    std::shared_ptr<TGranuleMeta> RegisterTable(
        const TInternalPathId pathId, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex) {
        auto infoEmplace = Tables.emplace(pathId, std::make_shared<TGranuleMeta>(pathId, *this, counters, versionedIndex));
        YDB_LOG_VERIFY(infoEmplace.second);
        return infoEmplace.first->second;
    }

    bool EraseTable(const TInternalPathId pathId) {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return false;
        }
        if (!it->second->IsErasable()) {
            return false;
        }
        Tables.erase(it);
        return true;
    }

    const THashMap<TInternalPathId, std::shared_ptr<TGranuleMeta>>& GetTables() const {
        return Tables;
    }

    void ReturnToIndexes(const THashMap<TInternalPathId, THashSet<ui64>>& portions) const {
        for (auto&& [g, portionIds] : portions) {
            auto it = Tables.find(g);
            YDB_LOG_VERIFY(it != Tables.end());
            it->second->ReturnToIndexes(portionIds);
        }
    }

    std::shared_ptr<TPortionInfo> GetPortionOptional(const TInternalPathId pathId, const ui64 portionId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second->GetPortionOptional(portionId);
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const TInternalPathId pathId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleVerified(const TInternalPathId pathId) const {
        auto it = Tables.find(pathId);
        YDB_LOG_VERIFY(it != Tables.end());
        return it->second;
    }

    const std::shared_ptr<IStoragesManager>& GetStoragesManager() const {
        return StoragesManager;
    }

    const NColumnShard::TEngineLogsCounters& GetCounters() const {
        return Counters;
    }

    /**
    Returns granules for compaction in descending order of priority
    */
    std::vector<TGranuleOrdered> GetGranulesForCompaction(const std::set<TInternalPathId>& pathIds = Default<std::set<TInternalPathId>>(),
        const std::optional<ui64> waitingPriority = std::nullopt) const;
};

}   // namespace NKikimr::NOlap
