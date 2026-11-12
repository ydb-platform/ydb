#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

#include <util/generic/function_ref.h>

#include <optional>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <typename TPortion>
struct CompactionTask {
    std::vector<typename TPortion::TConstPtr> Portions;
    ui8 TargetLevel;
    TOptimizationPriority Priority;
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

template <typename TPortion>
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

    std::optional<CompactionTask<TPortion>> GetNextOptimizationTask(TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
        return DoGetNextOptimizationTask(isLocked);
    }

    virtual std::optional<CompactionTask<TPortion>> DoGetNextOptimizationTask(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const = 0;

    virtual void DoActualize(const TInstant /*currentInstant*/) {
        return;
    };

    virtual TOptimizationPriority DoGetUsefulMetric() const = 0;

    virtual NJson::TJsonValue DoSerializeToJsonVisual() const = 0;

protected:
    virtual void DoAddPortion(typename TPortion::TPtr p) = 0;
    virtual void DoRemovePortion(typename TPortion::TConstPtr p) = 0;
};

template <typename TPortion>
struct TPortionByIndexKeyEndComparator {
    using is_transparent = void;   // Enable heterogeneous lookup
    using TKeyRef = typename TPortion::TKeyRef;

    bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
        return std::tuple(left->IndexKeyEndRef(), left->GetPortionId()) < std::tuple(right->IndexKeyEndRef(), right->GetPortionId());
    }

    bool operator()(const typename TPortion::TConstPtr& left, const TKeyRef& right) const {
        return left->IndexKeyEndRef() < right;
    }

    bool operator()(const TKeyRef& left, const typename TPortion::TConstPtr& right) const {
        return left < right->IndexKeyEndRef();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
