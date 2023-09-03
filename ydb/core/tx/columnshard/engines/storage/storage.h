#pragma once
#include "granule.h"
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap {

class TGranulesStorage {
private:
    const TCompactionLimits Limits;
    const NColumnShard::TEngineLogsCounters Counters;
    THashMap<ui64, TCompactionPriority> GranulesCompactionPriority;
    std::map<TCompactionPriority, std::set<ui64>> GranuleCompactionPrioritySorting;
    bool PackModificationFlag = false;
    THashMap<ui64, const TGranuleMeta*> PackModifiedGranules;
    void StartModificationImpl() {
        Y_VERIFY(!PackModificationFlag);
        PackModificationFlag = true;
    }

    void FinishModificationImpl() {
        Y_VERIFY(PackModificationFlag);
        PackModificationFlag = false;
        for (auto&& i : PackModifiedGranules) {
            UpdateGranuleInfo(*i.second);
        }
        PackModifiedGranules.clear();
    }

public:
    TGranulesStorage(const NColumnShard::TEngineLogsCounters counters, const TCompactionLimits& limits)
        : Limits(limits)
        , Counters(counters) {

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

    std::optional<ui64> GetGranuleForCompaction() const {
        if (!GranuleCompactionPrioritySorting.size()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules_for_compaction");
            return {};
        }
        for (auto it = GranuleCompactionPrioritySorting.rbegin(); it != GranuleCompactionPrioritySorting.rend(); ++it) {
            if (it->first.GetWeight() == 0) {
                break;
            }
            Y_VERIFY(it->second.size());
            for (auto&& i : it->second) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule")("granule_stats", it->first.DebugString())("granule_id", i);
                return i;
            }
        }
        return {};
    }

    void UpdateGranuleInfo(const TGranuleMeta& granule);

};

} // namespace NKikimr::NOlap
