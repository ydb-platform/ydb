#pragma once
#include "granule.h"
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NOlap {

class TGranulesStorage {
private:
    const NColumnShard::TEngineLogsCounters Counters;
    std::shared_ptr<IStoragesManager> StoragesManager;
    bool PackModificationFlag = false;
    THashMap<ui64, const TGranuleMeta*> PackModifiedGranules;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Tables; // pathId into Granule that equal to Table

    i64 MetadataMemoryPortionsSize = 0;

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

    static inline TAtomicCounter SumMetadataMemoryPortionsSize = 0;

public:
    TGranulesStorage(const NColumnShard::TEngineLogsCounters counters, const std::shared_ptr<IStoragesManager>& storagesManager)
        : Counters(counters)
        , StoragesManager(storagesManager)
    {

    }

    std::shared_ptr<TGranuleMeta> RegisterTable(const ui64 pathId, std::shared_ptr<TGranulesStorage> selfPtr, const NColumnShard::TGranuleDataCounters& counters, const TVersionedIndex& versionedIndex) {
        auto infoEmplace = Tables.emplace(pathId, std::make_shared<TGranuleMeta>(pathId, selfPtr, counters, versionedIndex));
        AFL_VERIFY(infoEmplace.second);
        return infoEmplace.first->second;
    }

    void EraseTable(const ui64 pathId) {
        auto it = Tables.find(pathId);
        Y_ABORT_UNLESS(it != Tables.end());
        Y_ABORT_UNLESS(it->second->IsErasable());
        Tables.erase(it);
    }

    const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& GetTables() const {
        return Tables;
    }

    void ReturnToIndexes(const THashMap<ui64, THashSet<ui64>>& portions) const {
        for (auto&& [g, portionIds] : portions) {
            auto it = Tables.find(g);
            AFL_VERIFY(it != Tables.end());
            it->second->ReturnToIndexes(portionIds);
        }
    }

    std::vector<std::shared_ptr<TGranuleMeta>> GetTables(const std::optional<ui64> pathIdFrom, const std::optional<ui64> pathIdTo) const {
        std::vector<std::shared_ptr<TGranuleMeta>> result;
        for (auto&& i : Tables) {
            if (pathIdFrom && i.first < *pathIdFrom) {
                continue;
            }
            if (pathIdTo && i.first > *pathIdTo) {
                continue;
            }
            result.emplace_back(i.second);
        }
        return result;
    }

    std::shared_ptr<TPortionInfo> GetPortionOptional(const ui64 pathId, const ui64 portionId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second->GetPortionOptional(portionId);
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const ui64 pathId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second;
    }

    ~TGranulesStorage() {
        SumMetadataMemoryPortionsSize.Sub(MetadataMemoryPortionsSize);
    }

    static ui64 GetSumMetadataMemoryPortionsSize() {
        return SumMetadataMemoryPortionsSize.Val();
    }

    i64 GetMetadataMemoryPortionsSize() const {
        return MetadataMemoryPortionsSize;
    }

    const std::shared_ptr<IStoragesManager>& GetStoragesManager() const {
        return StoragesManager;
    }

    const NColumnShard::TEngineLogsCounters& GetCounters() const {
        return Counters;
    }

    class TModificationGuard: TNonCopyable {
    private:
        TGranulesStorage& Owner;
    public:
        TModificationGuard(TGranulesStorage& storage)
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

    std::shared_ptr<TGranuleMeta> GetGranuleForCompaction(const std::shared_ptr<NDataLocks::TManager>& locksManager) const;

    void UpdateGranuleInfo(const TGranuleMeta& granule);
    void OnRemovePortion(const TPortionInfo& portion);
    void OnAddPortion(const TPortionInfo& portion);

};

} // namespace NKikimr::NOlap
