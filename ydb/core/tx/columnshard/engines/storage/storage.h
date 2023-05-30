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
            return {};
        }
        const TInstant now = TInstant::Now();
        std::optional<ui64> reserve;
        TInstant reserveInstant;
        for (auto it = GranuleCompactionPrioritySorting.rbegin(); it != GranuleCompactionPrioritySorting.rend(); ++it) {
            auto itSorting = GranuleCompactionPrioritySorting.rbegin();
            Y_VERIFY(itSorting->second.size());
            for (auto&& i : itSorting->second) {
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
