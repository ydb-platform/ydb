#pragma once
#include "granule.h"
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NOlap {

class TGranulesStorage {
private:
    const TCompactionLimits Limits;
    const NColumnShard::TEngineLogsCounters Counters;
    std::shared_ptr<IStoragesManager> StoragesManager;
    bool PackModificationFlag = false;
    THashMap<ui64, const TGranuleMeta*> PackModifiedGranules;
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
    TGranulesStorage(const NColumnShard::TEngineLogsCounters counters, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager)
        : Limits(limits)
        , Counters(counters)
        , StoragesManager(storagesManager)
    {

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

    std::shared_ptr<TGranuleMeta> GetGranuleForCompaction(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules) const;

    void UpdateGranuleInfo(const TGranuleMeta& granule);

};

} // namespace NKikimr::NOlap
