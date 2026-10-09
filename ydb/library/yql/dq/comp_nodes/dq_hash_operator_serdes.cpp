#include "dq_hash_operator_serdes.h"

#include <util/string/builder.h>

#include <vector>

namespace NKikimr {
namespace NMiniKQL {

using NDqHashOperatorCommon::IterateInputNodes;
using NDqHashOperatorCommon::NodesFromInputTuple;
using NDqHashOperatorCommon::ExternalNodesFromInputTuple;

namespace {

TType* UnwrapBlockType(TType* type)
{
    if (type->GetKind() == TType::EKind::Block) {
        return static_cast<const TBlockType*>(type)->GetItemType();
    }
    return type;
}

std::vector<TRuntimeNode> GetTupleNodes(TCallable& callable, ui32 inputIndex)
{
    const auto* tuple = AS_VALUE(TTupleLiteral, callable.GetInput(inputIndex));
    std::vector<TRuntimeNode> result;
    result.reserve(tuple->GetValuesCount());
    for (ui32 i = 0; i < tuple->GetValuesCount(); ++i) {
        result.push_back(tuple->GetValue(i));
    }
    return result;
}

void ParseFastFinalize(TCallable& callable, NDqHashOperatorCommon::TCombinerNodes& nodes)
{
    using namespace NDqHashOperatorCommon;

    const auto outputs = GetTupleNodes(callable, NDqHashOperatorParams::Finish);
    const auto keys = GetTupleNodes(callable, NDqHashOperatorParams::FinishKeyArgs);
    const auto states = GetTupleNodes(callable, NDqHashOperatorParams::FinishStateArgs);

    if (outputs.empty()) {
        nodes.FastFinalizeError = "Finalize has no outputs";
        return;
    }

    nodes.FastFinalize.reserve(outputs.size());
    for (ui32 outputIndex = 0; outputIndex < outputs.size(); ++outputIndex) {
        const auto output = outputs[outputIndex];
        bool found = false;
        for (ui32 keyIndex = 0; keyIndex < keys.size(); ++keyIndex) {
            if (output.GetNode() == keys[keyIndex].GetNode()) {
                nodes.FastFinalize.push_back({
                    .Source = EFastFinalizeSource::Key,
                    .SourceIndex = keyIndex,
                });
                found = true;
                break;
            }
        }
        if (found) {
            continue;
        }

        for (ui32 stateIndex = 0; stateIndex < states.size(); ++stateIndex) {
            if (output.GetNode() == states[stateIndex].GetNode()) {
                nodes.FastFinalize.push_back({
                    .Source = EFastFinalizeSource::State,
                    .SourceIndex = stateIndex,
                });
                found = true;
                break;
            }
        }
        if (!found) {
            nodes.FastFinalize.clear();
            nodes.FastFinalizeError = TStringBuilder()
                << "Finalize output " << outputIndex << " is not a direct key/state argument";
            return;
        }
    }
}

}

TDqHashOperatorParams ParseCommonDqHashOperatorParams(TCallable& callable, const TComputationNodeFactoryContext& ctx)
{
    MKQL_ENSURE(callable.GetInputsCount() >= 11U, "Expected more arguments.");

    TDqHashOperatorParams result;

    const TType* inputType = callable.GetInput(NDqHashOperatorParams::Input).GetStaticType();
    const TType* outputType = callable.GetType()->GetReturnType();

    result.IsStream = inputType->IsStream();

    const auto inputWidth = GetWideComponentsCount(inputType);
    const auto outputWidth = GetWideComponentsCount(outputType);

    const auto keysSize = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::KeyArgs))->GetValuesCount();
    const auto stateSize = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::StateArgs))->GetValuesCount();

    MKQL_ENSURE(result.IsStream == outputType->IsStream(), "Both the input and output types must be of the same kind (stream or flow)");

    result.InputWidth = inputWidth;
    result.KeyTypes.reserve(keysSize);
    result.KeyItemTypes.reserve(keysSize);
    result.StateItemTypes.reserve(stateSize);

    // extract types of the getKey and getInitialState lambdas
    IterateInputNodes(callable, NDqHashOperatorParams::GetKey, [&](TRuntimeNode rtNode) {
        TType *type = rtNode.GetStaticType();
        result.KeyItemTypes.push_back(type);
        bool optional;
        result.KeyTypes.emplace_back(*UnpackOptionalData(UnwrapBlockType(rtNode.GetStaticType()), optional)->GetDataSlot(), optional);
    });
    IterateInputNodes(callable, NDqHashOperatorParams::InitState, [&](TRuntimeNode rtNode) {
        TType *type = rtNode.GetStaticType();
        result.StateItemTypes.push_back(type);
    });

    NDqHashOperatorCommon::TCombinerNodes& nodes = result.Nodes;

    ParseFastFinalize(callable, nodes);

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

    nodes.FinishKeyNodes.reserve(keysSize);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::FinishKeyArgs, nodes.FinishKeyNodes);

    nodes.FinishStateNodes.reserve(keysSize);
    ExternalNodesFromInputTuple(ctx, callable, NDqHashOperatorParams::FinishStateArgs, nodes.FinishStateNodes);

    nodes.BuildMaps();

    return result;
}

}
}
