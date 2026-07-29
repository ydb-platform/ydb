#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <memory>

namespace NKikimr::NMiniKQL {

inline void SetFilterRow(TComputationContext& ctx, const TComputationExternalNodePtrVector& args,
                         const NUdf::TUnboxedValue* row) {
    for (size_t i = 0; i < args.size(); ++i) {
        args[i]->SetValue(ctx, NUdf::TUnboxedValue(row[i]));
    }
}

// NULL counts as "does not pass", per SQL ON semantics.
inline bool EvalFilterBody(TComputationContext& ctx, IComputationNode* body) {
    const NUdf::TUnboxedValue result = body->GetValue(ctx);
    return result && result.Get<bool>();
}

// Args are in original user column order. Body == nullptr means the filter is absent.
struct TJoinFilter {
    TComputationExternalNodePtrVector Args;
    IComputationNode* Body = nullptr;

    explicit operator bool() const {
        return Body != nullptr;
    }

    bool Pass(TComputationContext& ctx, const NUdf::TUnboxedValue* row) const {
        SetFilterRow(ctx, Args, row);
        return EvalFilterBody(ctx, Body);
    }
};

struct TJoinCommonFilter {
    TComputationExternalNodePtrVector LeftArgs;
    TComputationExternalNodePtrVector RightArgs;
    IComputationNode* Body = nullptr;

    explicit operator bool() const {
        return Body != nullptr;
    }

    bool Pass(TComputationContext& ctx, const NUdf::TUnboxedValue* leftRow,
              const NUdf::TUnboxedValue* rightRow) const {
        SetFilterRow(ctx, LeftArgs, leftRow);
        SetFilterRow(ctx, RightArgs, rightRow);
        return EvalFilterBody(ctx, Body);
    }
};

struct TJoinFilters {
    TJoinFilter Left;
    TJoinFilter Right;
    TJoinCommonFilter Common;

    explicit operator bool() const {
        return Left || Right || Common;
    }
};

// Consecutive callable inputs from firstIndex, where an empty args tuple means the filter is absent:
// leftArgs, leftBody, rightArgs, rightBody, commonLeftArgs, commonRightArgs, commonBody.
inline constexpr ui32 JoinFilterInputs = 7;

inline bool LocateFilterArgs(const TComputationNodeFactoryContext& ctx, TCallable& callable, ui32 index,
                             TComputationExternalNodePtrVector& args) {
    const auto tuple = AS_VALUE(TTupleLiteral, callable.GetInput(index));
    args.reserve(tuple->GetValuesCount());
    for (ui32 i = 0; i < tuple->GetValuesCount(); ++i) {
        auto* external = dynamic_cast<IComputationExternalNode*>(
            LocateNode(ctx.NodeLocator, *tuple->GetValue(i).GetNode(), /*pop=*/true));
        MKQL_ENSURE(external, "Expected an external node as a join filter argument");
        args.push_back(external);
    }
    return !args.empty();
}

inline TJoinFilters ParseJoinFilters(const TComputationNodeFactoryContext& ctx, TCallable& callable, ui32 firstIndex) {
    TJoinFilters filters;
    if (LocateFilterArgs(ctx, callable, firstIndex, filters.Left.Args)) {
        filters.Left.Body = LocateNode(ctx.NodeLocator, callable, firstIndex + 1);
    }
    if (LocateFilterArgs(ctx, callable, firstIndex + 2, filters.Right.Args)) {
        filters.Right.Body = LocateNode(ctx.NodeLocator, callable, firstIndex + 3);
    }
    const bool hasLeft = LocateFilterArgs(ctx, callable, firstIndex + 4, filters.Common.LeftArgs);
    const bool hasRight = LocateFilterArgs(ctx, callable, firstIndex + 5, filters.Common.RightArgs);
    if (hasLeft || hasRight) {
        filters.Common.Body = LocateNode(ctx.NodeLocator, callable, firstIndex + 6);
    }
    return filters;
}

// Decides whether a matched pair passes the ON-clause predicates. Probe is the left input, Build the
// right one. `columnPermutation` is the join's per-side "keys first" reordering (packed position i
// holds original column columnPermutation[i]); empty means identity.
class TPackedTuplePairFilter {
  public:
    TPackedTuplePairFilter(TComputationContext& ctx, TSides<std::unique_ptr<IScalarLayoutConverter>> converters,
                           TSides<TVector<int>> columnPermutation, TSides<int> widths, TJoinFilters filters)
        : Ctx_(&ctx)
        , Converters_(std::move(converters))
        , ColumnPermutation_(std::move(columnPermutation))
        , Filters_(std::move(filters))
        , NeedLeft_(Filters_.Left || Filters_.Common)
        , NeedRight_(Filters_.Right || Filters_.Common)
    {
        for (ESide side : EachSide) {
            ValsPermuted_.SelectSide(side).resize(widths.SelectSide(side));
            ValsOrig_.SelectSide(side).resize(widths.SelectSide(side));
        }
    }

    bool operator()(TSides<TSingleTuple> pair) {
        const NUdf::TUnboxedValue* leftRow = nullptr;
        if (NeedLeft_) {
            if (pair.Probe.PackedData != LastProbe_) {
                LastProbe_ = pair.Probe.PackedData;
                LeftRow_ = Decode(ESide::Probe, pair.Probe);
                LeftPassed_ = !Filters_.Left || Filters_.Left.Pass(*Ctx_, LeftRow_);
            }
            if (!LeftPassed_) {
                return false;
            }
            leftRow = LeftRow_;
        }
        if (!NeedRight_) {
            return true;
        }
        const NUdf::TUnboxedValue* rightRow = Decode(ESide::Build, pair.Build);
        if (Filters_.Right && !Filters_.Right.Pass(*Ctx_, rightRow)) {
            return false;
        }
        return !Filters_.Common || Filters_.Common.Pass(*Ctx_, leftRow, rightRow);
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
    TJoinFilters Filters_;
    const bool NeedLeft_;
    const bool NeedRight_;
    TSides<TPackResult> OneTuple_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsPermuted_;
    TSides<TVector<NUdf::TUnboxedValue>> ValsOrig_;
    const ui8* LastProbe_ = nullptr;
    const NUdf::TUnboxedValue* LeftRow_ = nullptr;
    bool LeftPassed_ = false;
};

} // namespace NKikimr::NMiniKQL
