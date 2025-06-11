#include "dq_hash_operator_serdes.h"

namespace NKikimr {
namespace NMiniKQL {

using NDqHashOperatorCommon::IterateInputNodes;
using NDqHashOperatorCommon::NodesFromInputTuple;
using NDqHashOperatorCommon::ExternalNodesFromInputTuple;

TDqHashOperatorParams ParseCommonDqHashOperatorParams(TCallable& callable, const TComputationNodeFactoryContext& ctx)
{
    MKQL_ENSURE(callable.GetInputsCount() >= 11U, "Expected more arguments.");

    const auto inputType = AS_TYPE(TFlowType, callable.GetInput(NDqHashOperatorParams::Flow).GetStaticType());
    const auto inputWidth = GetWideComponentsCount(inputType);
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));

    const auto keysSize = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::KeyArgs))->GetValuesCount();
    const auto stateSize = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::StateArgs))->GetValuesCount();

    TDqHashOperatorParams result;

    result.KeyTypes.reserve(keysSize);
    result.KeyAndStateItemTypes.reserve(keysSize + stateSize);

    // extract types of the getKey and getInitialState lambdas
    IterateInputNodes(callable, NDqHashOperatorParams::GetKey, [&](TRuntimeNode rtNode) {
        TType *type = rtNode.GetStaticType();
        result.KeyAndStateItemTypes.push_back(type);
        bool optional;
        result.KeyTypes.emplace_back(*UnpackOptionalData(rtNode.GetStaticType(), optional)->GetDataSlot(), optional);
    });
    IterateInputNodes(callable, NDqHashOperatorParams::InitState, [&](TRuntimeNode rtNode) {
        TType *type = rtNode.GetStaticType();
        result.KeyAndStateItemTypes.push_back(type);
    });

    NDqHashOperatorCommon::TCombinerNodes& nodes = result.Nodes;

    // extract result nodes of the all the input lambdas (getKey, initState, updateState, finish)
    nodes.KeyResultNodes.reserve(keysSize);
    NodesFromInputTuple(ctx, callable, NDqHashOperatorParams::GetKey, nodes.KeyResultNodes);

    nodes.InitResultNodes.reserve(stateSize);
    NodesFromInputTuple(ctx, callable, NDqHashOperatorParams::InitState, nodes.InitResultNodes);

    nodes.UpdateResultNodes.reserve(stateSize);
    NodesFromInputTuple(ctx, callable, NDqHashOperatorParams::UpdateState, nodes.UpdateResultNodes);

    nodes.FinishResultNodes.reserve(outputWidth);
    NodesFromInputTuple(ctx, callable, NDqHashOperatorParams::Finish, nodes.FinishResultNodes);

    // extract arguments of the input lambdas (input row item args, key args, state args, keys+state arguments to the final output lambda)
    nodes.KeyNodes.reserve(keysSize);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::KeyArgs, nodes.KeyNodes);

    nodes.StateNodes.reserve(stateSize);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::StateArgs, nodes.StateNodes);

    nodes.ItemNodes.reserve(inputWidth);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::ItemArgs, nodes.ItemNodes);

    nodes.FinishNodes.reserve(keysSize + stateSize);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::FinishKeyArgs, nodes.FinishNodes);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::FinishStateArgs, nodes.FinishNodes);

    nodes.BuildMaps();

    return result;
}

}
}