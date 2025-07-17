#pragma once

#include "dq_hash_operator_common.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NDqHashOperatorCommon {

template<typename THandler>
void IterateInputNodes(TCallable& callable, const ui32 inputIndex, THandler&& handler)
{
    auto rtNodeTuple = AS_VALUE(TTupleLiteral, callable.GetInput(inputIndex));
    MKQL_ENSURE_S(rtNodeTuple != nullptr);
    for (ui32 i = 0; i < rtNodeTuple->GetValuesCount(); ++i) {
        handler(rtNodeTuple->GetValue(i));
    }
}

template<typename TContainer>
void NodesFromInputTuple(const TComputationNodeFactoryContext& ctx, TCallable& callable, const ui32 inputIndex, TContainer& container)
{
    IterateInputNodes(callable, inputIndex, [&](TRuntimeNode rtNode) {
        auto nodePtr = rtNode.GetNode();
        MKQL_ENSURE_S(nodePtr != nullptr);
        auto locatedNode = ctx.NodeLocator(nodePtr, false);
        MKQL_ENSURE_S(locatedNode != nullptr);
        container.push_back(locatedNode);
    });
}

template<typename TContainer>
void ExternalNodesFromInputTuple(const TComputationNodeFactoryContext& ctx, TCallable& callable, const ui32 inputIndex, TContainer& container)
{
    IterateInputNodes(callable, inputIndex, [&](TRuntimeNode rtNode) {
        auto nodePtr = rtNode.GetNode();
        MKQL_ENSURE_S(nodePtr != nullptr);
        auto locatedNode = dynamic_cast<IComputationExternalNode*>(ctx.NodeLocator(nodePtr, true));
        MKQL_ENSURE_S(locatedNode != nullptr);
        container.push_back(locatedNode);
    });
}

} // namespace NDqHashOperatorCommon

// Has to be consistent with TDqProgramBuilder::BuildCommonCombinerParams
namespace NDqHashOperatorParams
{
    enum : ui32 {
        Input = 0,
        OperatorParams = 1,
        KeyArgs = 2,
        StateArgs = 3,
        ItemArgs = 4,
        GetKey = 5,
        InitState = 6,
        UpdateState = 7,
        FinishKeyArgs = 8,
        FinishStateArgs = 9,
        Finish = 10,
    };

    enum : ui32 {
        CombineParamMemLimit = 0,
    };

    enum : ui32 {
        AggregateParamEnableSpilling = 0,
    };
};

struct TDqHashOperatorParams
{
    size_t InputWidth = 0;
    TKeyTypes KeyTypes;
    std::vector<TType*> KeyItemTypes;
    std::vector<TType*> StateItemTypes;
    NDqHashOperatorCommon::TCombinerNodes Nodes;
};

TDqHashOperatorParams ParseCommonDqHashOperatorParams(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}