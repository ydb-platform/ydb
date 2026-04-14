#pragma once

#include <ydb/core/tx/columnshard/engines/portions/portion_slice_concept.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <util/generic/function_ref.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct CompactionTask {
    std::vector<typename TPortion::TConstPtr> Portions;
};

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct ICompactionUnit {
    const TLevelCounters& Counters;

    ICompactionUnit(const TLevelCounters& counters)
        : Counters(counters) {
    }

    virtual void AddPortion(typename TPortion::TConstPtr p) {
        Counters.Portions->AddPortion(p);
        DoAddPortion(p);
    }
    virtual void RemovePortion(typename TPortion::TConstPtr p) {
        Counters.Portions->RemovePortion(p);
        DoRemovePortion(p);
    }

    std::vector<CompactionTask<TKey, TPortion>> GetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
        return DoGetOptimizationTasks(isLocked);
    }

    virtual std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const = 0;
    virtual void DoActualize() = 0;
    virtual TOptimizationPriority DoGetUsefulMetric() const = 0;

private:
    virtual void DoAddPortion(typename TPortion::TConstPtr p) = 0;
    virtual void DoRemovePortion(typename TPortion::TConstPtr p) = 0;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
