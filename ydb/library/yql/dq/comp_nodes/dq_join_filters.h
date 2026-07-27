#pragma once

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/join_defs.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <memory>

namespace NKikimr::NMiniKQL {

struct TJoinFilter {
    TComputationExternalNodePtrVector Args;
    IComputationNode* Body = nullptr;

    explicit operator bool() const {
        return Body != nullptr;
    }

    bool Pass(TComputationContext& ctx, const NUdf::TUnboxedValue* row) const {
        for (size_t i = 0; i < Args.size(); ++i) {
            Args[i]->SetValue(ctx, NUdf::TUnboxedValue(row[i]));
        }
        const NUdf::TUnboxedValue result = Body->GetValue(ctx);
        return result && result.Get<bool>();
    }
};

struct TJoinCommonFilter {
    TComputationExternalNodePtrVector LeftArgs;
    TComputationExternalNodePtrVector RightArgs;
    IComputationNode* Body = nullptr;

    explicit operator bool() const {
        return Body != nullptr;
    }

    bool Pass(TComputationContext& ctx, const NUdf::TUnboxedValue* leftRow, const NUdf::TUnboxedValue* rightRow) const {
        for (size_t i = 0; i < LeftArgs.size(); ++i) {
            LeftArgs[i]->SetValue(ctx, NUdf::TUnboxedValue(leftRow[i]));
        }
        for (size_t i = 0; i < RightArgs.size(); ++i) {
            RightArgs[i]->SetValue(ctx, NUdf::TUnboxedValue(rightRow[i]));
        }
        const NUdf::TUnboxedValue result = Body->GetValue(ctx);
        return result && result.Get<bool>();
    }
};

inline bool LocateJoinFilterArgs(const TComputationNodeFactoryContext& ctx, TCallable& callable, ui32 argsIndex,
                                 TComputationExternalNodePtrVector& args) {
    const auto argsTuple = AS_VALUE(TTupleLiteral, callable.GetInput(argsIndex));
    const ui32 count = argsTuple->GetValuesCount();
    args.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        auto* external = dynamic_cast<IComputationExternalNode*>(
            LocateNode(ctx.NodeLocator, *argsTuple->GetValue(i).GetNode(), /*pop=*/true));
        MKQL_ENSURE(external, "Expected an external node as a join filter argument");
        args.push_back(external);
    }
    return count != 0;
}

inline IComputationNode* LocateJoinFilterBody(const TComputationNodeFactoryContext& ctx, TCallable& callable,
                                              ui32 bodyIndex) {
    return LocateNode(ctx.NodeLocator, callable, bodyIndex);
}

inline TJoinFilter ParseJoinFilter(const TComputationNodeFactoryContext& ctx, TCallable& callable, ui32 argsIndex,
                                   ui32 bodyIndex) {
    TJoinFilter filter;
    if (LocateJoinFilterArgs(ctx, callable, argsIndex, filter.Args)) {
        filter.Body = LocateJoinFilterBody(ctx, callable, bodyIndex);
    }
    return filter;
}

inline TJoinCommonFilter ParseJoinCommonFilter(const TComputationNodeFactoryContext& ctx, TCallable& callable,
                                               ui32 leftArgsIndex, ui32 rightArgsIndex, ui32 bodyIndex) {
    TJoinCommonFilter filter;
    const bool hasLeft = LocateJoinFilterArgs(ctx, callable, leftArgsIndex, filter.LeftArgs);
    const bool hasRight = LocateJoinFilterArgs(ctx, callable, rightArgsIndex, filter.RightArgs);
    if (hasLeft || hasRight) {
        filter.Body = LocateJoinFilterBody(ctx, callable, bodyIndex);
    }
    return filter;
}

// The three parsed filters for one hash join, shared by the block and scalar wrappers.
struct TJoinFilters {
    TJoinFilter Left;
    TJoinFilter Right;
    TJoinCommonFilter Common;

    explicit operator bool() const {
        return Left || Right || Common;
    }
};

inline TJoinFilters ParseJoinFilters(const TComputationNodeFactoryContext& ctx, TCallable& callable,
                                     ui32 leftArgsIndex, ui32 leftBodyIndex, ui32 rightArgsIndex, ui32 rightBodyIndex,
                                     ui32 commonLeftArgsIndex, ui32 commonRightArgsIndex, ui32 commonBodyIndex) {
    return {
        .Left = ParseJoinFilter(ctx, callable, leftArgsIndex, leftBodyIndex),
        .Right = ParseJoinFilter(ctx, callable, rightArgsIndex, rightBodyIndex),
        .Common = ParseJoinCommonFilter(ctx, callable, commonLeftArgsIndex, commonRightArgsIndex, commonBodyIndex),
    };
}

// Evaluates the non-equi join filters on a matched (Build, Probe) pair for packed-tuple hash joins
// (both block and scalar). Probe == left input, Build == right input. Each side is decoded from its
// packed tuple into scalar rows (in original user column order) via a scalar converter that shares
// the join's packed layout, and only if some filter actually references that side.
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
            // The probe row is constant across all matches of one lookup, so decode and evaluate the
            // left filter at most once per probe.
            if (pair.Probe.PackedData != LastProbe_) {
                LastProbe_ = pair.Probe.PackedData;
                LeftRow_ = Decode(ESide::Probe, pair.Probe);
                LeftFilterPassed_ = Filters_.Left ? Filters_.Left.Pass(*Ctx_, LeftRow_) : true;
            }
            if (Filters_.Left && !LeftFilterPassed_) {
                return false;
            }
            leftRow = LeftRow_;
        }
        if (NeedRight_) {
            const NUdf::TUnboxedValue* rightRow = Decode(ESide::Build, pair.Build);
            if (Filters_.Right && !Filters_.Right.Pass(*Ctx_, rightRow)) {
                return false;
            }
            if (Filters_.Common) {
                return Filters_.Common.Pass(*Ctx_, leftRow, rightRow);
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
    TJoinFilters Filters_;
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
