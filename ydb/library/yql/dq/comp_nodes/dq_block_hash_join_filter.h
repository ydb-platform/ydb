#pragma once

#include "dq_join_filters.h"

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <memory>

namespace NKikimr::NMiniKQL {

// Evaluates the non-equi join filters on a matched (Build, Probe) pair for the block hash join.
// Probe == left input, Build == right input (standard orientation only). Each side is decoded from
// its packed tuple into scalar rows (in original user column order) via a scalar converter that
// shares the block converter's packed layout, and only if some filter actually references that side.
//
// The three predicates are applied at their natural granularity within the match loop:
//   - left filter  : depends only on the probe row, which is constant across all of a probe's
//                    matches, so it is decoded/evaluated once per probe (memoized) and short-circuits
//                    the whole pair on failure (for LEFT joins this yields the correct null-padding);
//   - right filter : depends only on the build row, evaluated per pair;
//   - common filter: depends on both rows, evaluated per pair.
//
// `columnPermutation` is the join's per-side "keys first" reordering (packed position i holds the
// original user column columnPermutation[i]); empty means identity. `widths` is the number of data
// columns per side.
class TBlockJoinPairFilter {
  public:
    TBlockJoinPairFilter(TComputationContext& ctx, TSides<std::unique_ptr<IScalarLayoutConverter>> converters,
                         TSides<TVector<int>> columnPermutation, TSides<int> widths, TJoinFilter leftFilter,
                         TJoinFilter rightFilter, TJoinCommonFilter commonFilter)
        : Ctx_(&ctx)
        , Converters_(std::move(converters))
        , ColumnPermutation_(std::move(columnPermutation))
        , LeftFilter_(std::move(leftFilter))
        , RightFilter_(std::move(rightFilter))
        , CommonFilter_(std::move(commonFilter))
        , NeedLeft_(LeftFilter_ || CommonFilter_)
        , NeedRight_(RightFilter_ || CommonFilter_)
    {
        for (ESide side : EachSide) {
            ValsPermuted_.SelectSide(side).resize(widths.SelectSide(side));
            ValsOrig_.SelectSide(side).resize(widths.SelectSide(side));
        }
    }

    bool operator()(TSides<TSingleTuple> pair) {
        const NUdf::TUnboxedValue* leftRow = nullptr;
        if (NeedLeft_) {
            // The probe row is constant across all matches of one lookup, so decode and evaluate the
            // left filter at most once per probe.
            if (pair.Probe.PackedData != LastProbe_) {
                LastProbe_ = pair.Probe.PackedData;
                LeftRow_ = Decode(ESide::Probe, pair.Probe);
                LeftFilterPassed_ = LeftFilter_ ? LeftFilter_.Pass(*Ctx_, LeftRow_) : true;
            }
            if (LeftFilter_ && !LeftFilterPassed_) {
                return false;
            }
            leftRow = LeftRow_;
        }
        if (NeedRight_) {
            const NUdf::TUnboxedValue* rightRow = Decode(ESide::Build, pair.Build);
            if (RightFilter_ && !RightFilter_.Pass(*Ctx_, rightRow)) {
                return false;
            }
            if (CommonFilter_) {
                return CommonFilter_.Pass(*Ctx_, leftRow, rightRow);
            }
        }
        return true;
    }

  private:
    const NUdf::TUnboxedValue* Decode(ESide side, TSingleTuple tuple) {
        auto& converter = *Converters_.SelectSide(side);
        auto& one = OneTuple_.SelectSide(side);
        one.Clear();
        one.AppendTuple(tuple, converter.GetTupleLayout());
        auto& permuted = ValsPermuted_.SelectSide(side);
        converter.Unpack(one, 0, permuted.data());

        const auto& perm = ColumnPermutation_.SelectSide(side);
        if (perm.empty()) {
            return permuted.data();
        }
        auto& orig = ValsOrig_.SelectSide(side);
        for (size_t i = 0; i < permuted.size(); ++i) {
            orig[perm[i]] = permuted[i];
        }
        return orig.data();
    }

    TComputationContext* Ctx_;
    TSides<std::unique_ptr<IScalarLayoutConverter>> Converters_;
    TSides<TVector<int>> ColumnPermutation_;
    TJoinFilter LeftFilter_;
    TJoinFilter RightFilter_;
    TJoinCommonFilter CommonFilter_;
    const bool NeedLeft_;
    const bool NeedRight_;
    TSides<TPackResult> OneTuple_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsPermuted_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsOrig_;
    // Probe-decode memoization: the last probe tuple's packed pointer and its decoded row / left
    // filter verdict, reused across all matches of that probe.
    const ui8* LastProbe_ = nullptr;
    const NUdf::TUnboxedValue* LeftRow_ = nullptr;
    bool LeftFilterPassed_ = false;
};

} // namespace NKikimr::NMiniKQL
