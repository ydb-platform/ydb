#pragma once
#include "loaded_key_ranges.h"

#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <utility>

namespace NKikimr::NOlap {
class TGranuleMeta;
}

namespace NKikimr::NOlap::NGranule::NPortionsIndex {

class TPortionsIndex {
public:
    using TPortions = std::vector<TPortionInfo::TConstPtr>;
    using TKeyInterval = std::pair<NArrow::TSimpleRow, NArrow::TSimpleRow>;

    struct TPortionsSnapshot {
        using TConstPtr = std::shared_ptr<const TPortionsSnapshot>;

        TPortions Portions;
        TLoadedKeyRanges LoadedKeyRanges;

        // Returns merged key ranges of overlapping portions outside the task that may hold older records.
        // If loaded keys do not cover the input, the result covers the whole input.
        std::vector<TKeyInterval> FindOverlappingKeyRanges(const TPortionInfo& inputPortion, const THashSet<ui64>& skipPortions) const;
    };

private:
    THashMap<ui64, std::shared_ptr<TPortionInfo>> Portions;
    const TGranuleMeta& Owner;

public:
    TPortionsIndex(const TGranuleMeta& owner, const NColumnShard::TPortionsIndexCounters& /* counters */)
        : Owner(owner)
    {
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        YDB_LOG_VERIFY(p);
        YDB_LOG_VERIFY(Portions.emplace(p->GetPortionId(), p).second);
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        YDB_LOG_VERIFY(p);
        YDB_LOG_VERIFY(Portions.erase(p->GetPortionId()));
    }

    TPortionsSnapshot::TConstPtr GetPortionsSnapshot() const;
};

}   // namespace NKikimr::NOlap::NGranule::NPortionsIndex
