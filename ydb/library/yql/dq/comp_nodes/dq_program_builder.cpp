#include "dq_program_builder.h"
#include "type_utils.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {


TDqProgramBuilder::TDqProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry)
    : TProgramBuilder(env, functionRegistry) {}

TCallableBuilder TDqProgramBuilder::BuildCommonCombinerParams(
    const TStringBuf operatorName,
    const TRuntimeNode operatorParams,
    const TRuntimeNode flow,
    const TProgramBuilder::TWideLambda& keyExtractor,
    const TProgramBuilder::TBinaryWideLambda& init,
    const TProgramBuilder::TTernaryWideLambda& update,
    const TProgramBuilder::TBinaryWideLambda& finish)
{
    const auto wideComponents = GetWideComponents(AS_TYPE(TStreamType, flow.GetStaticType()));

    std::vector<TType*> unblockedWideComponents;
    bool hasBlocks = UnwrapBlockTypes(wideComponents, unblockedWideComponents);
    if (hasBlocks) {
        unblockedWideComponents.pop_back(); // Block height parameter
    }

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(unblockedWideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), unblockedWideComponents.size(), [&](){ return Arg(unblockedWideComponents[i++]); });

    const auto keys = keyExtractor(itemArgs);

    TRuntimeNode::TList keyArgs;
    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    const auto first = init(keyArgs, itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto next = update(keyArgs, itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    TRuntimeNode::TList finishKeyArgs;
    finishKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(finishKeyArgs), [&](TRuntimeNode key){ return Arg(key.GetStaticType()); }  );

    TRuntimeNode::TList finishStateArgs;
    finishStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(finishStateArgs), [&](TRuntimeNode state){ return Arg(state.GetStaticType()); }  );

    const auto output = finish(finishKeyArgs, finishStateArgs);

    std::vector<TType*> outputWideComponents;
    outputWideComponents.reserve(output.size() + hasBlocks ? 1 : 0);
    std::transform(output.cbegin(), output.cend(), std::back_inserter(outputWideComponents), std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));
    if (hasBlocks) {
        WrapArrayBlockTypes(outputWideComponents, *this);
        auto blockSizeType = NewDataType(NUdf::TDataType<ui64>::Id);
        auto blockSizeBlockType = NewBlockType(blockSizeType, TBlockType::EShape::Scalar);
        outputWideComponents.push_back(blockSizeBlockType);
    }

    TCallableBuilder callableBuilder(GetTypeEnvironment(), operatorName, NewStreamType(NewMultiType(outputWideComponents)));

    callableBuilder.Add(flow);
    callableBuilder.Add(operatorParams);
    callableBuilder.Add(NewTuple(keyArgs));
    callableBuilder.Add(NewTuple(stateArgs));
    callableBuilder.Add(NewTuple(itemArgs));
    callableBuilder.Add(NewTuple(keys));
    callableBuilder.Add(NewTuple(first));
    callableBuilder.Add(NewTuple(next));
    callableBuilder.Add(NewTuple(finishKeyArgs));
    callableBuilder.Add(NewTuple(finishStateArgs));
    callableBuilder.Add(NewTuple(output));

    return callableBuilder;
}

TRuntimeNode TDqProgramBuilder::DqHashCombine(TRuntimeNode flow, ui64 memLimit, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish)
{
    TRuntimeNode::TList operatorParamsList;
    operatorParamsList.push_back(NewDataLiteral<ui64>(memLimit));
    TRuntimeNode operatorParams = NewTuple(operatorParamsList);

    TCallableBuilder callableBuilder = BuildCommonCombinerParams(
        "DqHashCombine"sv,
        operatorParams,
        flow,
        keyExtractor,
        init,
        update,
        finish);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::DqHashAggregate(TRuntimeNode flow, const bool spilling, const TWideLambda& keyExtractor, const TBinaryWideLambda& init, const TTernaryWideLambda& update, const TBinaryWideLambda& finish)
{
    TRuntimeNode::TList operatorParamsList;
    operatorParamsList.push_back(NewDataLiteral<bool>(spilling));
    TRuntimeNode operatorParams = NewTuple(operatorParamsList);

    TCallableBuilder callableBuilder = BuildCommonCombinerParams(
        "DqHashAggregate"sv,
        operatorParams,
        flow,
        keyExtractor,
        init,
        update,
        finish);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::DqBlockHashJoin(TRuntimeNode leftStream, TRuntimeNode rightStream, EJoinKind joinKind,
    const TArrayRef<const ui32>& leftKeyColumns, const TArrayRef<const ui32>& rightKeyColumns, TType* returnType) {

    MKQL_ENSURE(joinKind == EJoinKind::Inner, "Unsupported join kind");
    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key column count mismatch");
    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");

    // TODO (mfilitov): add validation like here:
    // https://github.com/ydb-platform/ydb/blob/e8af538b05a1bd7bc4a3bcba2fdcbe430675f69c/yql/essentials/minikql/mkql_program_builder.cpp#L5849

    TRuntimeNode::TList leftKeyColumnsNodes;
    leftKeyColumnsNodes.reserve(leftKeyColumns.size());
    std::transform(leftKeyColumns.cbegin(), leftKeyColumns.cend(),
        std::back_inserter(leftKeyColumnsNodes), [this](const ui32 idx) {
            return NewDataLiteral(idx);
        });

    TRuntimeNode::TList rightKeyColumnsNodes;
    rightKeyColumnsNodes.reserve(rightKeyColumns.size());
    std::transform(rightKeyColumns.cbegin(), rightKeyColumns.cend(),
        std::back_inserter(rightKeyColumnsNodes), [this](const ui32 idx) {
            return NewDataLiteral(idx);
        });


    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(leftStream);
    callableBuilder.Add(rightStream);
    callableBuilder.Add(NewDataLiteral((ui32)joinKind));
    callableBuilder.Add(NewTuple(leftKeyColumnsNodes));
    callableBuilder.Add(NewTuple(rightKeyColumnsNodes));

    return TRuntimeNode(callableBuilder.Build(), false);
}

}
}
