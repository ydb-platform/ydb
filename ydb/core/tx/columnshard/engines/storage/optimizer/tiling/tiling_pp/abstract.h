#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <util/generic/function_ref.h>

#include <optional>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
struct CompactionTask {
    std::vector<typename TPortion::TConstPtr> Portions;
    ui8 TargetLevel;
};

template <typename TPortion>
struct TPortionByIdComparator {
    using is_transparent = void;

    bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
        return left->GetPortionId() < right->GetPortionId();
    }

    bool operator()(const typename TPortion::TConstPtr& left, const ui64 right) const {
        return left->GetPortionId() < right;
    }

    bool operator()(const ui64 left, const typename TPortion::TConstPtr& right) const {
        return left < right->GetPortionId();
    }
};

template <std::totally_ordered TKey, typename TPortion>
struct ICompactionUnit {
    using TLevelCounters = NTiling::TLevelCounters;

    const TLevelCounters& Counters;

    ICompactionUnit(const TLevelCounters& counters)
        : Counters(counters)
    {
    }

    virtual void AddPortion(typename TPortion::TPtr p) {
        if constexpr (std::is_same_v<TPortion, NOlap::TPortionInfo>) {
            Counters.Portions->AddPortion(p);
        }
        DoAddPortion(p);
    }

    virtual void RemovePortion(typename TPortion::TConstPtr p) {
        if constexpr (std::is_same_v<TPortion, NOlap::TPortionInfo>) {
            Counters.Portions->RemovePortion(p);
        }
        DoRemovePortion(p);
    }

    std::vector<CompactionTask<TKey, TPortion>> GetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
        return DoGetOptimizationTasks(isLocked);
    }

    std::optional<CompactionTask<TKey, TPortion>> GetNextOptimizationTask(TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
        return DoGetNextOptimizationTask(isLocked);
    }

    virtual std::optional<CompactionTask<TKey, TPortion>> DoGetNextOptimizationTask(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const = 0;

    virtual std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
        if (const auto task = DoGetNextOptimizationTask(isLocked)) {
            return { *task };
        }
        return {};
    }

    virtual void DoActualize() = 0;
    virtual TOptimizationPriority DoGetUsefulMetric() const = 0;

protected:
    virtual void DoAddPortion(typename TPortion::TPtr p) = 0;
    virtual void DoRemovePortion(typename TPortion::TConstPtr p) = 0;
};

template <std::totally_ordered TKey, typename TPortion>
struct TPortionByIndexKeyEndComparator {
    using is_transparent = void;   // Enable heterogeneous lookup

    bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
        return std::tuple(left->IndexKeyEnd(), left->GetPortionId()) < std::tuple(right->IndexKeyEnd(), right->GetPortionId());
    }

    bool operator()(const typename TPortion::TConstPtr& left, const TKey& right) const {
        return left->IndexKeyEnd() < right;
    }

    bool operator()(const TKey& left, const typename TPortion::TConstPtr& right) const {
        return left < right->IndexKeyEnd();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
