#include "dq_program_builder.h"
#include "type_utils.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

TDqProgramBuilder::TDqProgramBuilder(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry)
    : TProgramBuilder(env, functionRegistry)
{}

TCallableBuilder TDqProgramBuilder::BuildCommonCombinerParams(
    const TStringBuf operatorName, const TRuntimeNode operatorParams, const TRuntimeNode flowOrStream,
    const TProgramBuilder::TWideLambda& keyExtractor, const TProgramBuilder::TBinaryWideLambda& init,
    const TProgramBuilder::TTernaryWideLambda& update, const TProgramBuilder::TBinaryWideLambda& finish)
{
    const auto wideComponents = GetWideComponents(flowOrStream.GetStaticType());
    const bool isFlow = flowOrStream.GetStaticType()->IsFlow();

    std::vector<TType*> unblockedWideComponents;
    bool hasBlocks = UnwrapBlockTypes(wideComponents, unblockedWideComponents);
    if (hasBlocks) {
        unblockedWideComponents.pop_back(); // Block height parameter
    }

    TRuntimeNode::TList itemArgs;
    itemArgs.reserve(unblockedWideComponents.size());

    auto i = 0U;
    std::generate_n(std::back_inserter(itemArgs), unblockedWideComponents.size(),
                    [&]() { return Arg(unblockedWideComponents[i++]); });

    const auto keys = keyExtractor(itemArgs);

    TRuntimeNode::TList keyArgs;
    keyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(keyArgs),
                   [&](TRuntimeNode key) { return Arg(key.GetStaticType()); });

    const auto first = init(keyArgs, itemArgs);

    TRuntimeNode::TList stateArgs;
    stateArgs.reserve(first.size());
    std::transform(first.cbegin(), first.cend(), std::back_inserter(stateArgs),
                   [&](TRuntimeNode state) { return Arg(state.GetStaticType()); });

    const auto next = update(keyArgs, itemArgs, stateArgs);
    MKQL_ENSURE(next.size() == first.size(), "Mismatch init and update state size.");

    TRuntimeNode::TList finishKeyArgs;
    finishKeyArgs.reserve(keys.size());
    std::transform(keys.cbegin(), keys.cend(), std::back_inserter(finishKeyArgs),
                   [&](TRuntimeNode key) { return Arg(key.GetStaticType()); });

    TRuntimeNode::TList finishStateArgs;
    finishStateArgs.reserve(next.size());
    std::transform(next.cbegin(), next.cend(), std::back_inserter(finishStateArgs),
                   [&](TRuntimeNode state) { return Arg(state.GetStaticType()); });

    const auto output = finish(finishKeyArgs, finishStateArgs);

    std::vector<TType*> outputWideComponents;
    outputWideComponents.reserve(output.size() + hasBlocks ? 1 : 0);
    std::transform(output.cbegin(), output.cend(), std::back_inserter(outputWideComponents),
                   std::bind(&TRuntimeNode::GetStaticType, std::placeholders::_1));
    if (hasBlocks) {
        WrapArrayBlockTypes(outputWideComponents, *this);
        auto blockSizeType = NewDataType(NUdf::TDataType<ui64>::Id);
        auto blockSizeBlockType = NewBlockType(blockSizeType, TBlockType::EShape::Scalar);
        outputWideComponents.push_back(blockSizeBlockType);
    }

    TType* const returnWideType = NewMultiType(outputWideComponents);
    TType* const returnType = isFlow ? NewFlowType(returnWideType) : NewStreamType(returnWideType);
    TCallableBuilder callableBuilder(GetTypeEnvironment(), operatorName, returnType);

    callableBuilder.Add(flowOrStream);
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

TRuntimeNode TDqProgramBuilder::DqHashCombine(TRuntimeNode flow, ui64 memLimit, const TWideLambda& keyExtractor,
                                              const TBinaryWideLambda& init, const TTernaryWideLambda& update,
                                              const TBinaryWideLambda& finish)
{
    TRuntimeNode::TList operatorParamsList;
    operatorParamsList.push_back(NewDataLiteral<ui64>(memLimit));
    TRuntimeNode operatorParams = NewTuple(operatorParamsList);

    TCallableBuilder callableBuilder =
        BuildCommonCombinerParams("DqHashCombine"sv, operatorParams, flow, keyExtractor, init, update, finish);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::DqHashAggregate(TRuntimeNode flow, const bool spilling, const TWideLambda& keyExtractor,
                                                const TBinaryWideLambda& init, const TTernaryWideLambda& update,
                                                const TBinaryWideLambda& finish)
{
    TRuntimeNode::TList operatorParamsList;
    operatorParamsList.push_back(NewDataLiteral<bool>(spilling));
    TRuntimeNode operatorParams = NewTuple(operatorParamsList);

    TCallableBuilder callableBuilder =
        BuildCommonCombinerParams("DqHashAggregate"sv, operatorParams, flow, keyExtractor, init, update, finish);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::AsTuple(TArrayRef<const ui32> nums) {
    TRuntimeNode::TList tupleNodes;
    std::transform(nums.cbegin(), nums.cend(), std::back_inserter(tupleNodes), [this](const ui32 idx) {
        return NewDataLiteral(idx);
    });
    return NewTuple(tupleNodes);
}

TRuntimeNode TDqProgramBuilder::DqBlockHashJoin(TRuntimeNode leftStream, TRuntimeNode rightStream, EJoinKind joinKind,
                                                const TArrayRef<const ui32>& leftKeyColumns,
                                                const TArrayRef<const ui32>& rightKeyColumns,
                                                const TArrayRef<const ui32>& leftRenames,
                                                const TArrayRef<const ui32>& rightRenames, TType* returnType,
                                                TBlockHashJoinSettings settings,
                                                const TScalarJoinFilterLambda& leftFilter,
                                                const TScalarJoinFilterLambda& rightFilter,
                                                const TScalarJoinCommonFilterLambda& commonFilter) {

    MKQL_ENSURE(joinKind != EJoinKind::Cross, "Unsupported join kind");
    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key column count mismatch");
    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");

    const bool hasFilters = leftFilter || rightFilter || commonFilter;
    MKQL_ENSURE(!hasFilters || !settings.LeftIsBuild(), "Join filters are not supported with LeftIsBuild block join");

    // Filters are evaluated per matched pair at runtime, so all three ON-clause predicates collapse
    // into a single (leftRow, rightRow) -> Bool lambda. Its arguments are the item types of the data
    // columns (the last wide component is the block-length scalar). For a LEFT join the right side is
    // decoded as Optional, so its argument types are wrapped to match.
    const auto makeScalarRowArgs = [this](TRuntimeNode stream, bool wrapOptional) {
        const auto components = GetWideComponents(stream.GetStaticType());
        MKQL_ENSURE(!components.empty(), "Expected at least the block-length column");
        TRuntimeNode::TList args;
        args.reserve(components.size() - 1);
        for (size_t i = 0; i + 1 < components.size(); ++i) {
            TType* itemType = AS_TYPE(TBlockType, components[i])->GetItemType();
            if (wrapOptional && !itemType->IsOptional()) {
                itemType = NewOptionalType(itemType);
            }
            args.push_back(Arg(itemType));
        }
        return args;
    };

    // Absent filters are encoded as empty argument tuples plus a constant-true body.
    TRuntimeNode filterLeftArgs = NewEmptyTuple();
    TRuntimeNode filterRightArgs = NewEmptyTuple();
    TRuntimeNode filterBody = NewDataLiteral<bool>(true);
    if (hasFilters) {
        const bool needLeft = leftFilter || commonFilter;
        const bool needRight = rightFilter || commonFilter;
        const auto leftArgs = needLeft ? makeScalarRowArgs(leftStream, /*wrapOptional=*/false) : TRuntimeNode::TList{};
        const auto rightArgs =
            needRight ? makeScalarRowArgs(rightStream, /*wrapOptional=*/joinKind == EJoinKind::Left) : TRuntimeNode::TList{};

        TRuntimeNode::TList parts;
        if (leftFilter) {
            parts.push_back(leftFilter(leftArgs));
        }
        if (rightFilter) {
            parts.push_back(rightFilter(rightArgs));
        }
        if (commonFilter) {
            parts.push_back(commonFilter(leftArgs, rightArgs));
        }
        filterBody = parts.size() == 1 ? parts.front() : And(parts);
        if (needLeft) {
            filterLeftArgs = NewTuple(leftArgs);
        }
        if (needRight) {
            filterRightArgs = NewTuple(rightArgs);
        }
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(leftStream);
    callableBuilder.Add(rightStream);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(joinKind)));
    callableBuilder.Add(AsTuple(leftKeyColumns));
    callableBuilder.Add(AsTuple(rightKeyColumns));
    callableBuilder.Add(AsTuple(leftRenames));
    callableBuilder.Add(AsTuple(rightRenames));
    callableBuilder.Add(NewTuple({NewDataLiteral(static_cast<ui32>(settings.BuildSide))}));
    callableBuilder.Add(filterLeftArgs);
    callableBuilder.Add(filterRightArgs);
    callableBuilder.Add(filterBody);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::DqScalarHashJoin(TRuntimeNode leftFlow, TRuntimeNode rightFlow, EJoinKind joinKind,
                                                 const TArrayRef<const ui32>& leftKeyColumns,
                                                 const TArrayRef<const ui32>& rightKeyColumns,
                                                 const TArrayRef<const ui32>& leftRenames,
                                                 const TArrayRef<const ui32>& rightRenames, TType* returnType,
                                                 const TScalarJoinFilterLambda& leftFilter,
                                                 const TScalarJoinFilterLambda& rightFilter,
                                                 const TScalarJoinCommonFilterLambda& commonFilter) {

    MKQL_ENSURE(joinKind != EJoinKind::Cross, "Unsupported join kind");
    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key column count mismatch");
    MKQL_ENSURE(!leftKeyColumns.empty(), "At least one key column must be specified");

    const auto leftComponents = GetWideComponents(leftFlow.GetStaticType());
    const auto rightComponents = GetWideComponents(rightFlow.GetStaticType());

    // Build a wide-row argument list (one Arg per input column) for a side.
    const auto makeRowArgs = [this](const TArrayRef<TType* const>& components) {
        TRuntimeNode::TList args;
        args.reserve(components.size());
        for (auto* type : components) {
            args.push_back(Arg(type));
        }
        return args;
    };

    // Absent filters are encoded as an empty argument tuple plus a constant-true body, so the
    // runtime wrapper can cheaply detect and skip them (see WrapDqScalarHashJoin).
    TRuntimeNode leftFilterArgs = NewEmptyTuple();
    TRuntimeNode leftFilterBody = NewDataLiteral<bool>(true);
    if (leftFilter) {
        const auto args = makeRowArgs(leftComponents);
        leftFilterBody = leftFilter(args);
        leftFilterArgs = NewTuple(args);
    }

    TRuntimeNode rightFilterArgs = NewEmptyTuple();
    TRuntimeNode rightFilterBody = NewDataLiteral<bool>(true);
    if (rightFilter) {
        const auto args = makeRowArgs(rightComponents);
        rightFilterBody = rightFilter(args);
        rightFilterArgs = NewTuple(args);
    }

    TRuntimeNode commonFilterLeftArgs = NewEmptyTuple();
    TRuntimeNode commonFilterRightArgs = NewEmptyTuple();
    TRuntimeNode commonFilterBody = NewDataLiteral<bool>(true);
    if (commonFilter) {
        const auto leftArgs = makeRowArgs(leftComponents);
        const auto rightArgs = makeRowArgs(rightComponents);
        commonFilterBody = commonFilter(leftArgs, rightArgs);
        commonFilterLeftArgs = NewTuple(leftArgs);
        commonFilterRightArgs = NewTuple(rightArgs);
    }

    TCallableBuilder callableBuilder(Env, __func__, returnType);
    callableBuilder.Add(leftFlow);
    callableBuilder.Add(rightFlow);
    callableBuilder.Add(NewDataLiteral(static_cast<ui32>(joinKind)));
    callableBuilder.Add(AsTuple(leftKeyColumns));
    callableBuilder.Add(AsTuple(rightKeyColumns));
    callableBuilder.Add(AsTuple(leftRenames));
    callableBuilder.Add(AsTuple(rightRenames));
    callableBuilder.Add(leftFilterArgs);
    callableBuilder.Add(leftFilterBody);
    callableBuilder.Add(rightFilterArgs);
    callableBuilder.Add(rightFilterBody);
    callableBuilder.Add(commonFilterLeftArgs);
    callableBuilder.Add(commonFilterRightArgs);
    callableBuilder.Add(commonFilterBody);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TRuntimeNode TDqProgramBuilder::DqWatermarkGenerator(
    TRuntimeNode input,
    const TUnaryLambda& watermarkExtractor,
    const TUnaryLambda& partitionKeyExtractor,
    const TUnaryLambda& writeTimeExtractor,
    TConstArrayRef<std::pair<std::string, std::string>> watermarkSettings,
    TRuntimeNode partitionKeys
) {
    auto returnType = AS_TYPE(TStreamType, input);
    auto itemType = returnType->GetItemType();

    const auto itemArg = Arg(itemType);
    const auto watermark = watermarkExtractor(itemArg);
    const auto partitionKey = partitionKeyExtractor(itemArg);
    const auto writeTime = writeTimeExtractor(itemArg);

    TRuntimeNode::TList watermarkSettingItems;
    for (const auto& [name, value] : watermarkSettings) {
        watermarkSettingItems.push_back(NewDataLiteral<NUdf::EDataSlot::String>(name));
        watermarkSettingItems.push_back(NewDataLiteral<NUdf::EDataSlot::String>(value));
    }
    const auto watermarkSettingsNode = NewList(NewDataType(NUdf::EDataSlot::String), watermarkSettingItems);

    TCallableBuilder callableBuilder(Env_, __func__, returnType);
    callableBuilder.Add(input);
    callableBuilder.Add(itemArg);
    callableBuilder.Add(watermark);
    callableBuilder.Add(partitionKey);
    callableBuilder.Add(writeTime);
    callableBuilder.Add(watermarkSettingsNode);
    callableBuilder.Add(partitionKeys);

    return TRuntimeNode(callableBuilder.Build(), false);
}

TType* TDqProgramBuilder::LastScalarIndexBlock() {
    return NewBlockType(NewDataType(NUdf::TDataType<ui64>::Id), TBlockType::EShape::Scalar);
}

} // namespace NMiniKQL
} // namespace NKikimr
