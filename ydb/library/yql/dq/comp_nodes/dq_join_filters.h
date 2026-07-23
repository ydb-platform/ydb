#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

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

} // namespace NKikimr::NMiniKQL
