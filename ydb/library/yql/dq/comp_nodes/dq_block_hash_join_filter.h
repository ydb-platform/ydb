#pragma once

#include "dq_join_filters.h"

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <memory>

namespace NKikimr::NMiniKQL {

class TBlockJoinPairFilter {
  public:
    TBlockJoinPairFilter(TComputationContext& ctx, TSides<std::unique_ptr<IScalarLayoutConverter>> converters,
                         TSides<TVector<int>> columnPermutation, TSides<int> widths, TJoinCommonFilter filter)
        : Ctx_(&ctx)
        , Converters_(std::move(converters))
        , ColumnPermutation_(std::move(columnPermutation))
        , Filter_(std::move(filter))
    {
        for (ESide side : EachSide) {
            ValsPermuted_.SelectSide(side).resize(widths.SelectSide(side));
            ValsOrig_.SelectSide(side).resize(widths.SelectSide(side));
        }
    }

    bool operator()(TSides<TSingleTuple> pair) {
        const NUdf::TUnboxedValue* leftRow = Filter_.LeftArgs.empty() ? nullptr : Decode(ESide::Probe, pair.Probe);
        const NUdf::TUnboxedValue* rightRow = Filter_.RightArgs.empty() ? nullptr : Decode(ESide::Build, pair.Build);
        return Filter_.Pass(*Ctx_, leftRow, rightRow);
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
    TJoinCommonFilter Filter_;
    TSides<TPackResult> OneTuple_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsPermuted_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsOrig_;
};

} // namespace NKikimr::NMiniKQL
