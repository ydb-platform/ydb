#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionsIndex {
public:
    using TPortions = std::vector<TPortionInfo::TConstPtr>;
    using TPortionsSnapshot = std::shared_ptr<const TPortions>;

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

    TPortionsSnapshot GetPortionsSnapshot() const {
        auto result = std::make_shared<TPortions>();
        result->reserve(Portions.size());
        for (const auto& [_, portion] : Portions) {
            result->emplace_back(portion);
        }
        return result;
    }

    using TKeyInterval = std::pair<NArrow::TSimpleRow, NArrow::TSimpleRow>;

    // Primary key ranges of the portions in `portions` that intersect `inputPortion`, are not listed in `skipPortions` and may hold
    // records older than the ones in `inputPortion`. Deletion markers of `inputPortion` are still required inside these ranges.
    // The result is sorted by range start and intersecting ranges are merged.
    static std::vector<TKeyInterval> GetOlderIntervals(
        const TPortions& portions, const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions);
};

}   // namespace NKikimr::NOlap::NGranule::NPortionsIndex
