#pragma once
#include "granule.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NOlap {

class TGranulesStat {
private:
    i64 MetadataMemoryPortionsSize = 0;
    const NColumnShard::TEngineLogsCounters Counters;
    bool PackModificationFlag = false;
    THashMap<TInternalPathId, const TGranuleMeta*> PackModifiedGranules;
    std::map<ui64, TPositiveControlInteger> SchemaVersionsControl;

    static inline TAtomicCounter SumMetadataMemoryPortionsSize = 0;

    void StartModificationImpl() {
        Y_ABORT_UNLESS(!PackModificationFlag);
        PackModificationFlag = true;
    }

    void FinishModificationImpl() {
        Y_ABORT_UNLESS(PackModificationFlag);
        PackModificationFlag = false;
        for (auto&& i : PackModifiedGranules) {
            UpdateGranuleInfo(*i.second);
        }
        PackModifiedGranules.clear();
    }

public:
    TGranulesStat(const NColumnShard::TEngineLogsCounters& counters)
        : Counters(counters) {
    }

    bool HasSchemaVersion(const ui64 fromVersion, const ui64 version) const {
        AFL_VERIFY(fromVersion <= version);
        auto it = SchemaVersionsControl.lower_bound(fromVersion);
        return (it != SchemaVersionsControl.end() && it->first <= version);
    }

    const NColumnShard::TEngineLogsCounters& GetCounters() const {
        return Counters;
    }

    class TModificationGuard: TNonCopyable {
    private:
        TGranulesStat& Owner;

    public:
        TModificationGuard(TGranulesStat& storage)
            : Owner(storage) {
            Owner.StartModificationImpl();
        }

        ~TModificationGuard() {
            Owner.FinishModificationImpl();
        }
    };

    TModificationGuard StartPackModification() {
        return TModificationGuard(*this);
    }

    static ui64 GetSumMetadataMemoryPortionsSize() {
        return SumMetadataMemoryPortionsSize.Val();
    }

    i64 GetMetadataMemoryPortionsSize() const {
        return MetadataMemoryPortionsSize;
    }

    ~TGranulesStat() {
        SumMetadataMemoryPortionsSize.Sub(MetadataMemoryPortionsSize);
    }

    void UpdateGranuleInfo(const TGranuleMeta& granule) {
        if (PackModificationFlag) {
            PackModifiedGranules[granule.GetPathId()] = &granule;
            return;
        }
    }

    void OnRemovePortion(const TPortionInfo& portion) {
        auto it = SchemaVersionsControl.find(portion.GetSchemaVersionVerified());
        AFL_VERIFY(it != SchemaVersionsControl.end());
        if (it->second.Dec() == 0) {
            SchemaVersionsControl.erase(it);
        }
        MetadataMemoryPortionsSize -= portion.GetMetadataMemorySize();
        AFL_VERIFY(MetadataMemoryPortionsSize >= 0);
        const i64 value = SumMetadataMemoryPortionsSize.Sub(portion.GetMetadataMemorySize());
        Counters.OnIndexMetadataUsageBytes(value);
    }

    void OnAddPortion(const TPortionInfo& portion) {
        SchemaVersionsControl[portion.GetSchemaVersionVerified()].Inc();
        MetadataMemoryPortionsSize += portion.GetMetadataMemorySize();
        const i64 value = SumMetadataMemoryPortionsSize.Add(portion.GetMetadataMemorySize());
        Counters.OnIndexMetadataUsageBytes(value);
    }
};

class TGranulesStorage {
private:
    const NColumnShard::TEngineLogsCounters Counters;
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> DataAccessorsManager;
    std::shared_ptr<IStoragesManager> StoragesManager;
    THashMap<TInternalPathId, std::shared_ptr<TGranuleMeta>> Tables;   // pathId into Granule that equal to Table
    std::shared_ptr<TGranulesStat> Stats;

public:
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& GetDataAccessorsManager() const {
        return DataAccessorsManager;
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
        const std::shared_ptr<IStoragesManager>& storagesManager)
        : Counters(counters)
        , DataAccessorsManager(dataAccessorsManager)
        , StoragesManager(storagesManager)
        , Stats(std::make_shared<TGranulesStat>(Counters)) {
        AFL_VERIFY(DataAccessorsManager);
        AFL_VERIFY(StoragesManager);
    }

    void FetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const {
        DataAccessorsManager->AskData(request);
    }

    const std::shared_ptr<TGranulesStat>& GetStats() const {
        return Stats;
    }

    std::shared_ptr<TGranuleMeta> RegisterTable(
        const TInternalPathId pathId, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex) {
        auto infoEmplace = Tables.emplace(pathId, std::make_shared<TGranuleMeta>(pathId, *this, counters, versionedIndex));
        AFL_VERIFY(infoEmplace.second);
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
            AFL_VERIFY(it != Tables.end());
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
        AFL_VERIFY(it != Tables.end());
        return it->second;
    }

    const std::shared_ptr<IStoragesManager>& GetStoragesManager() const {
        return StoragesManager;
    }

    const NColumnShard::TEngineLogsCounters& GetCounters() const {
        return Counters;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleForCompaction(const std::shared_ptr<NDataLocks::TManager>& locksManager) const;
    std::optional<NStorageOptimizer::TOptimizationPriority> GetCompactionPriority(const std::shared_ptr<NDataLocks::TManager>& locksManager,
        const std::set<TInternalPathId>& pathIds = Default<std::set<TInternalPathId>>(), const std::optional<ui64> waitingPriority = std::nullopt,
        std::shared_ptr<TGranuleMeta>* granuleResult = nullptr) const;
};

}   // namespace NKikimr::NOlap
