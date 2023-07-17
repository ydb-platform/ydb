#pragma once
#include "granule.h"
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap {

class TGranulesStorage {
private:
    const TCompactionLimits Limits;
    THashMap<ui64, THashSet<ui64>> PathsGranulesOverloaded;
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

    class TModificationGuard {
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

    ui32 GetOverloadedGranulesCount() const {
        ui32 result = 0;
        for (auto&& i : PathsGranulesOverloaded) {
            result += i.second.size();
        }
        return result;
    }

    bool HasOverloadedGranules() const {
        return PathsGranulesOverloaded.size();
    }

    TModificationGuard StartPackModification() {
        return TModificationGuard(*this);
    }

    const THashSet<ui64>* GetOverloaded(ui64 pathId) const;
    template <class TFilter>
    std::optional<ui64> GetGranuleForCompaction(const TFilter& filter) const {
        if (!GranuleCompactionPrioritySorting.size()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "no_granules_for_compaction");
            return {};
        }
        const TInstant now = TInstant::Now();
        std::optional<ui64> reserve;
        TInstant reserveInstant;
        for (auto it = GranuleCompactionPrioritySorting.rbegin(); it != GranuleCompactionPrioritySorting.rend(); ++it) {
            if (it->first.GetPriorityClass() == TGranuleAdditiveSummary::ECompactionClass::NoCompaction) {
                break;
            }
            Y_VERIFY(it->second.size());
            for (auto&& i : it->second) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "test_granule")("granule_stats", it->first.DebugString())("granule_id", i);
                if (filter(i)) {
                    if (it->first.GetNextAttemptInstant() > now) {
                        if (!reserve || reserveInstant > it->first.GetNextAttemptInstant()) {
                            reserveInstant = it->first.GetNextAttemptInstant();
                            reserve = i;
                        }
                    } else {
                        return i;
                    }
                }
            }
        }
        return reserve;
    }

    void UpdateGranuleInfo(const TGranuleMeta& granule);

};

} // namespace NKikimr::NOlap
