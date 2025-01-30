#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionsIndex {
private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    const TGranuleMeta& Owner;

public:
    TPortionsIndex(const TGranuleMeta& owner, const NColumnShard::TPortionsIndexCounters& /* counters */)
        : Owner(owner)
    {
        Y_UNUSED(Owner);
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        AFL_VERIFY(p);
        AFL_VERIFY(Portions.emplace(p->GetPortionId(), p).second);
    }
    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        AFL_VERIFY(p);
        AFL_VERIFY(Portions.erase(p->GetPortionId()));
    }

    bool HasOlderIntervals(const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const;
};


}