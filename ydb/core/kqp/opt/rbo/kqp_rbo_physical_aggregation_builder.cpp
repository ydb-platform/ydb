#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationInitialStateForOptionalTypePacked(TExprNode::TPtr asStruct, const TString& colName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrCountInit")
            .Callable(0, "Member")
                .Add(0, asStruct)
                .Atom(1, colName)
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrCountInit")
            .Add(0, lambdaArg)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationInitialState() {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Uint64")
            .Atom(0, "1")
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialStatePacked(TExprNode::TPtr asStruct, const TString& colName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .List()
            .Callable(0, "Convert")
                .Callable(0, "Member")
                    .Add(0, asStruct)
                    .Atom(1, colName)
                .Seal()
                .Atom(1, "Double")
            .Seal()
            .Callable(1, "Uint64")
                .Atom(0, "1")
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialState(TExprNode::TPtr lambdaArg) {
    // clang-format off
    return Ctx.Builder(Pos)
        .List()
            .Callable(0, "Convert")
                .Add(0, lambdaArg)
                .Atom(1, "Double")
            .Seal()
            .Callable(1, "Uint64")
                .Atom(0, "1")
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialStateForOptionalTypePacked(TExprNode::TPtr asStruct, const TString& colName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Callable(0, "Member")
                .Add(0, asStruct)
                .Atom(1, colName)
            .Seal()
            .Lambda(1)
                .Param("arg")
                .Callable(0, "Just")
                    .List(0)
                        .Callable(0, "Convert")
                            .Arg(0, "arg")
                            .Atom(1, "Double")
                        .Seal()
                        .Callable(1, "Uint64")
                            .Atom(0, "1")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Callable(0, "TupleType")
                        .Callable(0, "DataType")
                            .Atom(0, "Double")
                        .Seal()
                        .Callable(1, "DataType")
                            .Atom(0, "Uint64")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArg)
            .Lambda(1)
                .Param("arg")
                .Callable(0, "Just")
                    .List(0)
                        .Callable(0, "Convert")
                            .Arg(0, "arg")
                            .Atom(1, "Double")
                        .Seal()
                        .Callable(1, "Uint64")
                            .Atom(0, "1")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Callable(0, "TupleType")
                        .Callable(0, "DataType")
                            .Atom(0, "Double")
                        .Seal()
                        .Callable(1, "DataType")
                            .Atom(0, "Uint64")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TString TPhysicalAggregationBuilder::GetTypeToSafeCastForSumAggregation(const TTypeAnnotationNode* itemType) {
    Y_ENSURE(itemType);
    const auto* type = itemType;
    if (itemType->IsOptionalOrNull()) {
        type = itemType->Cast<TOptionalExprType>()->GetItemType();
    }

    Y_ENSURE(type->GetKind() == ETypeAnnotationKind::Data);
    const auto typeName = TString(type->Cast<TDataExprType>()->GetName());
    if (typeName.StartsWith("Int")) {
        return "Int64";
    } else if (typeName.StartsWith("Uint")) {
        return "Uint64";
    }

    return typeName;
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildSumAggregationInitialStatePacked(TExprNode::TPtr asStruct, const TString& colName, const TTypeAnnotationNode* itemType) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("SafeCast")
            .Callable(0, "Member")
                .Add(0, asStruct)
                .Atom(1, colName)
            .Seal()
            .Callable(1, "DataType")
                .Atom(0, GetTypeToSafeCastForSumAggregation(itemType))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildSumAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* itemType) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("SafeCast")
            .Add(0, lambdaArg)
            .Callable(1, "DataType")
                .Atom(0, GetTypeToSafeCastForSumAggregation(itemType))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationUpdateStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns,
                                                                                                   TExprNode::TPtr asStructInputColumns,
                                                                                                   const TString& stateColumn, const TString& columnName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrCountUpdate")
            .Callable(0, "Member")
                .Add(0, asStructInputColumns)
                .Atom(1, columnName)
            .Seal()
            .Callable(1, "Member")
                .Add(0, asStructStateColumns)
                .Atom(1, stateColumn)
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrCountUpdate")
            .Add(0, lambdaArgField)
            .Add(1, lambdaArgState)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, const TString& stateColumn) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Inc")
            .Callable(0, "Member")
                .Add(0, asStructStateColumns)
                .Atom(1, stateColumn)
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationUpdateState(TExprNode::TPtr lambdaArgState) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Inc")
            .Add(0, lambdaArgState)
    .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns,
                                                                                                 TExprNode::TPtr asStructInputColumns,
                                                                                                 const TString& stateColumn, const TString& columnName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Callable(0, "Member")
                .Add(0, asStructStateColumns)
                .Atom(1, stateColumn)
            .Seal()
            .Lambda(1)
                .Param("state_col_arg")
                .Callable(0, "IfPresent")
                    .Callable(0, "Member")
                        .Add(0, asStructInputColumns)
                        .Atom(1, columnName)
                    .Seal()
                    .Lambda(1)
                        .Param("input_col_arg")
                        .Callable(0, "Just")
                            .List(0)
                                .Callable(0, "AggrAdd")
                                    .Callable(0, "Nth")
                                        .Arg(0, "state_col_arg")
                                        .Atom(1, "0")
                                    .Seal()
                                    .Callable(1, "Convert")
                                        .Arg(0, "input_col_arg")
                                        .Atom(1, "Double")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Inc")
                                    .Callable(0, "Nth")
                                        .Arg(0, "state_col_arg")
                                        .Atom(1, "1")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(2, "Just")
                        .Arg(0, "state_col_arg")
                    .Seal()
                .Seal()
            .Seal()
            .Add(2, BuildAvgAggregationInitialStateForOptionalTypePacked(asStructInputColumns, columnName))
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns,
                                                                                  const TString& stateColumn, const TString& columnName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .List()
            .Callable(0, "AggrAdd")
                .Callable(0, "Nth")
                    .Callable(0, "Member")
                        .Add(0, asStructStateColumns)
                        .Atom(1, stateColumn)
                    .Seal()
                    .Atom(1, "0")
                .Seal()
                .Callable(1, "Convert")
                    .Callable(0, "Member")
                        .Add(0, asStrcutInputColumns)
                        .Atom(1, columnName)
                    .Seal()
                    .Atom(1, "Double")
                .Seal()
            .Seal()
            .Callable(1, "Inc")
                .Callable(0, "Nth")
                    .Callable(0, "Member")
                        .Add(0, asStructStateColumns)
                        .Atom(1, stateColumn)
                    .Seal()
                    .Atom(1, "1")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArgState)
            .Lambda(1)
                .Param("state_col_arg")
                .Callable(0, "IfPresent")
                    .Add(0, lambdaArgField)
                    .Lambda(1)
                        .Param("input_col_arg")
                        .Callable(0, "Just")
                            .List(0)
                                .Callable(0, "AggrAdd")
                                    .Callable(0, "Nth")
                                        .Arg(0, "state_col_arg")
                                        .Atom(1, "0")
                                    .Seal()
                                    .Callable(1, "Convert")
                                        .Arg(0, "input_col_arg")
                                        .Atom(1, "Double")
                                    .Seal()
                                .Seal()
                                .Callable(1, "Inc")
                                    .Callable(0, "Nth")
                                        .Arg(0, "state_col_arg")
                                        .Atom(1, "1")
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(2, "Just")
                        .Arg(0, "state_col_arg")
                    .Seal()
                .Seal()
            .Seal()
            .Add(2, BuildAvgAggregationInitialStateForOptionalType(lambdaArgField))
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField) {
    // clang-format off
    return Ctx.Builder(Pos)
        .List()
            .Callable(0, "AggrAdd")
                .Callable(0, "Nth")
                    .Add(0, lambdaArgState)
                    .Atom(1, "0")
                .Seal()
                .Callable(1, "Convert")
                    .Add(0, lambdaArgField)
                    .Atom(1, "Double")
                .Seal()
            .Seal()
            .Callable(1, "Inc")
                .Callable(0, "Nth")
                    .Add(0, lambdaArgState)
                    .Atom(1, "1")
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildSumAggregationUpdateStatePacked(TExprNode::TPtr asStructStateColumns, TExprNode::TPtr asStrcutInputColumns,
                                                                                  const TString& stateColumn, const TString& columnName,
                                                                                  const TTypeAnnotationNode* itemType) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Callable(0, "Member")
                .Add(0, asStructStateColumns)
                .Atom(1, stateColumn)
            .Seal()
            .Callable(1, "SafeCast")
                .Callable(0, "Member")
                    .Add(0, asStrcutInputColumns)
                    .Atom(1, columnName)
                .Seal()
                .Callable(1, "DataType")
                    .Atom(0, GetTypeToSafeCastForSumAggregation(itemType))
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildSumAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField,
                                                                            const TTypeAnnotationNode* itemType) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Add(0, lambdaArgState)
            .Callable(1, "SafeCast")
                .Add(0, lambdaArgField)
                .Callable(1, "DataType")
                    .Atom(0, GetTypeToSafeCastForSumAggregation(itemType))
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishStateForOptionalTypePacked(TExprNode::TPtr asStructStateColumns, const TString& stateName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Callable(0, "Member")
                .Add(0, asStructStateColumns)
                .Atom(1, stateName)
            .Seal()
            .Lambda(1)
                .Param("arg")
                .Callable(0, "Just")
                    .Callable(0, "Div")
                        .Callable(0, "Nth")
                            .Arg(0, "arg")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "arg")
                            .Atom(1, "1")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Callable(0, "DataType")
                        .Atom(0, "Double")
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr lambdaArgState) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArgState)
            .Lambda(1)
                .Param("arg")
                .Callable(0, "Just")
                    .Callable(0, "Div")
                        .Callable(0, "Nth")
                            .Arg(0, "arg")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "arg")
                            .Atom(1, "1")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Callable(0, "DataType")
                        .Atom(0, "Double")
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishStatePacked(TExprNode::TPtr asStructStateColumns, const TString& stateName) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Div")
            .Callable(0, "Nth")
                .Callable(0, "Member")
                    .Add(0, asStructStateColumns)
                    .Atom(1, stateName)
                .Seal()
                .Atom(1, "0")
            .Seal()
            .Callable(1, "Nth")
                .Callable(0, "Member")
                    .Add(0, asStructStateColumns)
                    .Atom(1, stateName)
                .Seal()
                .Atom(1, "1")
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishState(TExprNode::TPtr lambdaArgState) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Div")
            .Callable(0, "Nth")
                .Add(0, lambdaArgState)
                .Atom(1, "0")
            .Seal()
            .Callable(1, "Nth")
                .Add(0, lambdaArgState)
                .Atom(1, "1")
            .Seal()
        .Seal().Build();
    // clang-format on
}

// This lambda returns are keys for following aggregation.
// It has arguments in the following orders - inputs.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildKeyExtractorLambda(const TVector<TString>& keyFields, const TVector<TString>& inputColumns) {
    if (NeedToPackWideLambdas) {
        return BuildKeyExtractorLambdaPacked(keyFields, inputColumns);
    }

    // At fitst generate a lambda args, the size of args is equal to number of input columns.
    THashMap<TString, ui32> lambdaArgsMap;
    TVector<TExprNode::TPtr> lambdaArgs;
    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(i)));
        lambdaArgsMap.insert({inputColumns[i], i});
    }

    TVector<TExprNode::TPtr> lambdaResults;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        auto it = lambdaArgsMap.find(keyFields[i]);
        Y_ENSURE(it != lambdaArgsMap.end());
        lambdaResults.push_back(lambdaArgs[it->second]);
    }

    // Create a wide lambda - lambda with multiple outputs.
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildKeyExtractorLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputColumns) {
    // At fitst generate a lambda args, the size of args is equal to number of input columns.
    ui32 lambdaArgCounter = 0;
    TVector<TExprNode::TPtr> lambdaArgs;
    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgCounter++)));
    }

    // Pack all columns to struct.
    // clang-format off
    auto asStruct = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < inputColumns.size(); ++i) {
                parent.List(i)
                    .Atom(0, inputColumns[i])
                    .Add(1, lambdaArgs[i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();
    // clang-format on

    // Extract keys.
    TVector<TExprNode::TPtr> lambdaResults;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        // clang-format off
        auto member = Ctx.Builder(Pos)
            .Callable("Member")
                .Add(0, asStruct)
                .Atom(1, keyFields[i])
            .Seal().Build();
        // clang-format on
        lambdaResults.push_back(member);
    }

    // Create a wide lambda - lambda with multiple outputs.
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

// This lambdas initializes initial state for aggregation.
// It has arguments in the following order - keys, inputs.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildInitHandlerLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                          const TVector<TPhysicalAggregationTraits>& aggTraitsList) {
    // clang-format off
    const ui32 lambdaArgsSize = keyFields.size() + inputFields.size();
    TVector<TExprNode::TPtr> lambdaArgs;
    for (ui32 i = 0; i < lambdaArgsSize; ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(i)));
    }

    // clang-format off
    auto asStruct = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < inputFields.size(); ++i) {
                parent.List(i)
                    .Atom(0, inputFields[i])
                    .Add(1, lambdaArgs[keyFields.size() + i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();
    // clang-format on

    TVector<TExprNode::TPtr> lambdaResults;
    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFunction = aggTraits.AggFunc;
        const auto& aggName = aggTraits.AggFieldName;
        const auto isOptional = aggTraits.InputItemType->IsOptionalOrNull();

        TExprNode::TPtr initState;
        if (aggFunction == "count") {
            initState = isOptional ? BuildCountAggregationInitialStateForOptionalTypePacked(asStruct, aggName) : BuildCountAggregationInitialState();
        } else if (aggFunction == "avg") {
            initState = isOptional ? BuildAvgAggregationInitialStateForOptionalTypePacked(asStruct, aggName) : BuildAvgAggregationInitialStatePacked(asStruct, aggName);
        } else if (aggFunction == "sum") {
            initState = BuildSumAggregationInitialStatePacked(asStruct, aggName, aggTraits.InputItemType);
        } else {
            // clang-format off
            initState = Ctx.Builder(Pos)
                .Callable("Member")
                    .Add(0, asStruct)
                    .Atom(1, aggName)
            .Seal().Build();
            // clang-format on
        }
        lambdaResults.push_back(initState);
    }

    // Create a wide lambda - lambda with multiple outputs.
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

// This lambdas initializes initial state for aggregation.
// It has arguments in the following order - keys, inputs.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildInitHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                    const TVector<TPhysicalAggregationTraits>& aggTraitsList) {
    if (NeedToPackWideLambdas) {
        return BuildInitHandlerLambdaPacked(keyFields, inputFields, aggTraitsList);
    }

    ui32 lambdaArgsCounter = 0;
    THashMap<TString, ui32> lambdaArgsMap;
    TVector<TExprNode::TPtr> lambdaArgs;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({keyFields[i], lambdaArgsCounter++});
    }
    for (ui32 i = 0; i < inputFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({inputFields[i], lambdaArgsCounter++});
    }

    TVector<TExprNode::TPtr> lambdaResults;
    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFunction = aggTraits.AggFunc;
        const auto isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        const auto& aggName = aggTraits.AggFieldName;
        auto it = lambdaArgsMap.find(aggName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr initState = lambdaArgs[it->second];

        if (aggFunction == "count") {
            initState = isOptional ? BuildCountAggregationInitialStateForOptionalType(initState) : BuildCountAggregationInitialState();
        } else if (aggFunction == "avg") {
            initState = isOptional ? BuildAvgAggregationInitialStateForOptionalType(initState) : BuildAvgAggregationInitialState(initState);
        } else if (aggFunction == "sum") {
            initState = BuildSumAggregationInitialState(initState, aggTraits.InputItemType);
        }
        lambdaResults.push_back(initState);
    }

    // Create a wide lambda - lambda with multiple outputs.
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildUpdateHandlerLambdaPacked(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                            const TVector<TPhysicalAggregationTraits>& aggTraitsList) {
    ui32 lambdaArgsCounter = 0;
    TVector<TExprNode::TPtr> lambdaArgs;
    TVector<TExprNode::TPtr> keyArgs;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        keyArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter++)));
    }

    TVector<TExprNode::TPtr> inputArgs;
    for (ui32 i = 0; i < inputFields.size(); ++i) {
        inputArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter++)));
    }

    TVector<TExprNode::TPtr> stateArgs;
    for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
        stateArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter++)));
    }

    // clang-format off
    auto asStructInputColumns = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < inputFields.size(); ++i) {
                parent.List(i)
                    .Atom(0, inputFields[i])
                    .Add(1, inputArgs[i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();
    // clang-format on

    // clang-format off
    auto asStructStateColumns = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
                parent.List(i)
                    .Atom(0, aggTraitsList[i].StateFieldName)
                    .Add(1, stateArgs[i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();
    // clang-format on

    TVector<TExprNode::TPtr> lambdaResults;
    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFunction = aggTraits.AggFunc;
        const auto& columnName = aggTraits.AggFieldName;
        const auto& stateName = aggTraits.StateFieldName;
        const bool isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        TExprNode::TPtr phyAggFunc;

        if (aggFunction == "count") {
            phyAggFunc = isOptional ? BuildCountAggregationUpdateStateForOptionalTypePacked(asStructStateColumns, asStructInputColumns, stateName, columnName)
                                    : BuildCountAggregationUpdateStatePacked(asStructStateColumns, stateName);
        } else if (aggFunction == "distinct") {
            // clang-format off
            phyAggFunc = Ctx.Builder(Pos)
                .Callable("Member")
                    .Add(0, asStructStateColumns)
                    .Atom(1, stateName)
                .Seal().Build();
            // clang-format on
        } else if (aggFunction == "avg") {
            phyAggFunc = isOptional ? BuildAvgAggregationUpdateStateForOptionalTypePacked(asStructStateColumns, asStructInputColumns, stateName, columnName)
                                    : BuildAvgAggregationUpdateStatePacked(asStructStateColumns, asStructInputColumns, stateName, columnName);
        } else if (aggFunction == "sum") {
            phyAggFunc = BuildSumAggregationUpdateStatePacked(asStructStateColumns, asStructInputColumns, stateName, columnName, aggTraits.InputItemType);
        } else {
            auto it = AggregationFunctionToAggregationCallable.find(aggFunction);
            Y_ENSURE(it != AggregationFunctionToAggregationCallable.end());
            const auto& physicalAggregationFunctionName = it->second;
            // clang-format off
            phyAggFunc = Ctx.Builder(Pos)
                .Callable(physicalAggregationFunctionName)
                    .Callable(0, "Member")
                        .Add(0, asStructStateColumns)
                        .Atom(1, stateName)
                    .Seal()
                    .Callable(1, "Member")
                        .Add(0, asStructInputColumns)
                        .Atom(1, columnName)
                    .Seal()
            .Seal().Build();
            // clang-format on
        }
        lambdaResults.push_back(phyAggFunc);
    }

    lambdaArgs.insert(lambdaArgs.end(), keyArgs.begin(), keyArgs.end());
    lambdaArgs.insert(lambdaArgs.end(), inputArgs.begin(), inputArgs.end());
    lambdaArgs.insert(lambdaArgs.end(), stateArgs.begin(), stateArgs.end());

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

// This lambda performs an aggregation.
// It has arguments in the following order - keys, inputs, states.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildUpdateHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                      const TVector<TPhysicalAggregationTraits>& aggTraitsList) {
    if (NeedToPackWideLambdas) {
        return BuildUpdateHandlerLambdaPacked(keyFields, inputFields, aggTraitsList);
    }

    ui32 lambdaArgsCounter = 0;
    TVector<TExprNode::TPtr> lambdaArgs;
    THashMap<TString, ui32> lambdaArgsMap;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({keyFields[i], lambdaArgsCounter++});
    }
    for (ui32 i = 0; i < inputFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({inputFields[i], lambdaArgsCounter++});
    }
    for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({aggTraitsList[i].StateFieldName, lambdaArgsCounter++});
    }

    TVector<TExprNode::TPtr> lambdaResults;
    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFunction = aggTraits.AggFunc;
        const auto& fieldName = aggTraits.AggFieldName;
        const auto& stateName = aggTraits.StateFieldName;
        const bool isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        TExprNode::TPtr phyAggFunc;

        auto it = lambdaArgsMap.find(fieldName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr lambdaArgField = lambdaArgs[it->second];

        it = lambdaArgsMap.find(stateName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr lambdaArgState = lambdaArgs[it->second];

        if (aggFunction == "count") {
            phyAggFunc =
                isOptional ? BuildCountAggregationUpdateStateForOptionalType(lambdaArgState, lambdaArgState) : BuildCountAggregationUpdateState(lambdaArgState);
        } else if (aggFunction == "distinct") {
            phyAggFunc = lambdaArgState;
        } else if (aggFunction == "avg") {
            phyAggFunc = isOptional ? BuildAvgAggregationUpdateStateForOptionalType(lambdaArgState, lambdaArgField)
                                    : BuildAvgAggregationUpdateState(lambdaArgState, lambdaArgField);
        } else if (aggFunction == "sum") {
            phyAggFunc = BuildSumAggregationUpdateState(lambdaArgState, lambdaArgField, aggTraits.InputItemType);
        } else {
            auto it = AggregationFunctionToAggregationCallable.find(aggFunction);
            Y_ENSURE(it != AggregationFunctionToAggregationCallable.end());
            const auto& physicalAggregationFunctionName = it->second;
            // clang-format off
            phyAggFunc = Ctx.Builder(Pos)
                .Callable(physicalAggregationFunctionName)
                    .Add(0, lambdaArgField)
                    .Add(1, lambdaArgState)
                .Seal().Build();
            // clang-format on
        }
        lambdaResults.push_back(phyAggFunc);
    }

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildFinishHandlerLambdaPacked(const TVector<TString>& keyFields,
                                                                            const TVector<TPhysicalAggregationTraits>& aggTraitsList, bool distinctAll) {
    TVector<TExprNode::TPtr> lambdaKeyArgs;
    ui32 lambdaArgsCounter = 0;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        lambdaKeyArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter++)));
    }

    TVector<TExprNode::TPtr> lambdaStateArgs;
    for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
        lambdaStateArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter++)));
    }

    // clang-format off
    auto keyStruct = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < keyFields.size(); ++i) {
                parent.List(i)
                    .Atom(0, keyFields[i])
                    .Add(1, lambdaKeyArgs[i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();

    auto stateStruct = Ctx.Builder(Pos)
        .Callable("AsStruct")
        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
            for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
                parent.List(i)
                    .Atom(0, aggTraitsList[i].StateFieldName)
                    .Add(1, lambdaStateArgs[i])
                .Seal();
            }
            return parent;
        })
    .Seal().Build();
    // clang-format on

    TVector<TExprNode::TPtr> lambdaResults;
    // We do not need to return keys for distinct all
    if (!distinctAll) {
        for (ui32 i = 0; i < keyFields.size(); ++i) {
            // clang-format off
            auto member = Ctx.Builder(Pos)
                .Callable("Member")
                    .Add(0, keyStruct)
                    .Atom(1, keyFields[i])
                .Seal().Build();
            // clang-format on
            lambdaResults.push_back(member);
        }
    }

    for (const auto& aggTraits : aggTraitsList) {
        const TString& aggFuncName = aggTraits.AggFunc;
        const TString& stateName = aggTraits.StateFieldName;
        const bool isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        TExprNode::TPtr result;

        if (aggFuncName == "avg") {
            result = isOptional ? BuildAvgAggregationFinishStateForOptionalTypePacked(stateStruct, stateName)
                                : BuildAvgAggregationFinishStatePacked(stateStruct, stateName);
        } else {
            // clang-format off
            result = Ctx.Builder(Pos)
                .Callable("Member")
                    .Add(0, stateStruct)
                    .Atom(1, stateName)
                .Seal().Build();
            // clang-format on
        }
        lambdaResults.push_back(result);
    }

    lambdaKeyArgs.insert(lambdaKeyArgs.end(), lambdaStateArgs.begin(), lambdaStateArgs.end());
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaKeyArgs)), std::move(lambdaResults));
}

// This lambda returns aggregation result.
// It has arguments in the following order - keys, states.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildFinishHandlerLambda(const TVector<TString>& keyFields,
                                                                      const TVector<TPhysicalAggregationTraits>& aggTraitsList, bool distinctAll) {
    if (NeedToPackWideLambdas) {
        return BuildFinishHandlerLambdaPacked(keyFields, aggTraitsList, distinctAll);
    }

    ui32 lambdaArgsCounter = 0;
    TVector<TExprNode::TPtr> lambdaArgs;
    THashMap<TString, ui32> lambdaArgsMap;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({keyFields[i],  lambdaArgsCounter++});
    }
    for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({aggTraitsList[i].StateFieldName,  lambdaArgsCounter++});
    }

    TVector<TExprNode::TPtr> lambdaResults;
    // We do not need to return keys for distinct all
    if (!distinctAll) {
        for (ui32 i = 0; i < keyFields.size(); ++i) {
            auto it = lambdaArgsMap.find(keyFields[i]);
            lambdaResults.push_back(lambdaArgs[it->second]);
        }
    }

    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFuncName = aggTraits.AggFunc;
        const auto& stateName = aggTraits.StateFieldName;
        const bool isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        auto it = lambdaArgsMap.find(stateName);
        TExprNode::TPtr result = lambdaArgs[it->second];

        if (aggFuncName == "avg") {
            result = isOptional ? BuildAvgAggregationFinishStateForOptionalType(result) : BuildAvgAggregationFinishState(result);
        }
        lambdaResults.push_back(result);
    }

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildExpandMapForPhysicalAggregationInput(TExprNode::TPtr input, const TVector<TString>& inputColumns) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("ExpandMap")
            .Callable(0, "ToFlow")
                .Add(0, input)
            .Seal()
            .Lambda(1)
                .Param("narrow_input_param")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < inputColumns.size(); ++i) {
                        parent
                            .Callable(i, "Member")
                                .Arg(0, "narrow_input_param")
                                .Atom(1, inputColumns[i])
                            .Seal();
                    }
                    return parent;
                })
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildNarrowMapForPhysicalAggregationOutput(TExprNode::TPtr input, const TVector<TString>& keyFields,
                                                                                        const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                                                        const THashMap<TString, TString>& renameMap, bool distinctAll) {
    TVector<TString> outputFields;
    if (!distinctAll) {
        outputFields = keyFields;
    }
    for (const auto& aggTraits : aggTraitsList) {
        outputFields.push_back(aggTraits.StateFieldName);
    }

    if (keyFields.empty()) {
        // clang-format off
        input = Build<TCoTake>(Ctx, Pos)
            .Input(input)
            .Count<TCoUint64>()
                .Literal<TCoAtom>()
                    .Value("1")
                .Build()
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("wide_param", outputFields.size())
                .Callable(0, "AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < outputFields.size(); ++i) {
                        // Apply rename.
                        auto fieldName = outputFields[i];
                        auto it = renameMap.find(fieldName);
                        if (it != renameMap.end()) {
                            fieldName = it->second;
                        }
                        parent.List(i)
                            .Atom(0, fieldName)
                            .Arg(1, "wide_param", i)
                        .Seal();
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TVector<TString> TPhysicalAggregationBuilder::GetInputColumns(const TVector<TOpAggregationTraits>& aggregationTraitsList, const TVector<TInfoUnit>& keyColumns) {
    THashSet<TString> inputFields;
    for (const auto &aggTraits : aggregationTraitsList) {
        const auto fullName = aggTraits.OriginalColName.GetFullName();
        if (!inputFields.count(fullName)) {
            inputFields.insert(fullName);
        }
    }
    for (const auto &keyColumn: keyColumns) {
        const auto fullName = keyColumn.GetFullName();
        if (!inputFields.count(fullName)) {
            inputFields.insert(fullName);
        }
    }

    return TVector<TString>(inputFields.begin(), inputFields.end());
}

void TPhysicalAggregationBuilder::BuildPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TString>& keyColumns,
                                                                 const TVector<TOpAggregationTraits>& aggregationTraitsList, TVector<TString>& inputFields,
                                                                 TVector<TPhysicalAggregationTraits>& aggTraits, THashMap<TString, TString>& renameMap,
                                                                 const TTypeAnnotationNode* inputType, const TTypeAnnotationNode* outputType) {
    Y_ENSURE(inputType && outputType);
    const auto* inputStructType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto* outputStructType = outputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    THashMap<TString, TVector<std::tuple<TString, TString, const TTypeAnnotationNode*, const TTypeAnnotationNode*>>> aggColumns;
    for (const auto& aggregationTraits : aggregationTraitsList) {
        const TString originalColName = aggregationTraits.OriginalColName.GetFullName();
        const TString resultColName = aggregationTraits.ResultColName.GetFullName();
        const auto* inputItemType = inputStructType->FindItemType(originalColName);
        const auto* outputItemType = outputStructType->FindItemType(resultColName);
        Y_ENSURE(inputItemType && outputItemType, "Cannot find type for item");
        aggColumns[originalColName].push_back(std::make_tuple(aggregationTraits.AggFunction, resultColName, inputItemType, outputItemType));
    }

    THashMap<TString, TString> aggFieldsMap;
    THashSet<TString> keyColNames;
    keyColNames.insert(keyColumns.begin(), keyColumns.end());
    for (ui32 i = 0; i < inputColumns.size(); ++i) {
        const auto& originalColName = inputColumns[i];
        if (auto it = aggColumns.find(originalColName); it != aggColumns.end()) {
            const auto& aggFields = it->second;
            for (ui32 j = 0; j < aggFields.size(); ++j) {
                const auto& tupleTraits = aggFields[j];
                const auto& aggFunction = std::get<0>(tupleTraits);
                const auto& resultColName = std::get<1>(tupleTraits);
                const auto* inputType = std::get<2>(tupleTraits);
                const auto* outputType = std::get<3>(tupleTraits);

                auto stateName = "__kqp_agg_state_" + aggFunction + "_" + originalColName + ToString(j);
                // No renames for distinct, we want to process only keys.
                if (aggFunction == "distinct") {
                    stateName = originalColName;
                }

                TString inputField;
                if (!aggFieldsMap.contains(originalColName)) {
                    inputField = "__kqp_agg_input_col_" + originalColName + "_" + ToString(j);
                    inputFields.push_back(inputField);
                    aggFieldsMap[originalColName] = inputField;
                } else {
                    inputField = aggFieldsMap[originalColName];
                }

                aggTraits.emplace_back(inputField, stateName, aggFunction, inputType, outputType);
                // Map agg state name to result name.
                renameMap[stateName] = resultColName;
            }
        } else {
            TString inputField = originalColName;
            if (keyColNames.contains(originalColName)) {
                inputField = "__kqp_agg_input_key_" + originalColName + "_"  + ToString(i);
            }
            keyColNames.insert(inputField);
            inputFields.push_back(inputField);
        }
    }
}

TVector<TString> TPhysicalAggregationBuilder::GetKeyFields(const TVector<TInfoUnit>& keyColumns) {
    TVector<TString> keyFields;
    for (const auto& keyColumn : keyColumns) {
        keyFields.push_back(keyColumn.GetFullName());
    }
    return keyFields;
}

TExprNode::TPtr TPhysicalAggregationBuilder::CreateNothingForEmptyInput(const TTypeAnnotationNode* aggType) {
    Y_ENSURE(aggType);
    const auto* aggStructType = aggType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    // clang-format off
    return Build<TCoNothing>(Ctx, Pos)
        .OptionalType<TCoOptionalType>()
            .ItemType(ExpandType(Pos, *aggStructType, Ctx))
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::MapCondenseOutput(TExprNode::TPtr input, const TVector<TPhysicalAggregationTraits>& traits,
                                                               const THashMap<TString, TString>& renameMap) {
    // clang-format off
     return Ctx.Builder(Pos)
        .Callable("Map")
            .Add(0, input)
            .Lambda(1)
                .Param("arg")
                .Callable(0, "AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    for (ui32 i = 0; i < traits.size(); ++i) {
                        // Apply rename.
                        auto fieldName = traits[i].StateFieldName;
                        auto it = renameMap.find(fieldName);
                        if (it != renameMap.end()) {
                            fieldName = it->second;
                        }
                        const auto& aggFunc = traits[i].AggFunc;
                        if (aggFunc == "count") {
                            parent.List(i)
                                .Atom(0, fieldName)
                                .Callable(1, "Coalesce")
                                    .Callable(0, "Member")
                                        .Arg(0, "arg")
                                        .Atom(1, fieldName)
                                    .Seal()
                                    .Callable(1, "Uint64")
                                        .Atom(0, "0")
                                    .Seal()
                                .Seal()
                            .Seal();
                        } else {
                            parent.List(i)
                                .Atom(0, fieldName)
                                .Callable(1, "Member")
                                    .Arg(0, "arg")
                                    .Atom(1, fieldName)
                                .Seal()
                            .Seal();
                        }
                    }
                    return parent;
                })
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCondenseForAggregationOutputWithEmptyKeys(TExprNode::TPtr input,
                                                                                            const TVector<TPhysicalAggregationTraits>& traits,
                                                                                            const THashMap<TString, TString>& renameMap,
                                                                                            const TTypeAnnotationNode* type) {
    // clang-format off
    input = Build<TCoCondense>(Ctx, Pos)
        .Input(input)
        .State(CreateNothingForEmptyInput(type))
        .SwitchHandler()
            .Args({"item", "state"})
            .Body(MakeBool<false>(Pos, Ctx))
        .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoJust>()
                .Input("item")
            .Build()
        .Build()
    .Done().Ptr();
    // clang-format on

    return MapCondenseOutput(input, traits, renameMap);
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildPhysicalAggregation(TExprNode::TPtr input) {
    const auto& aggregationTraitsList = Aggregate->AggregationTraitsList;
    const auto& keyColumns = Aggregate->KeyColumns;
    const TVector<TString> inputColumns = GetInputColumns(aggregationTraitsList, keyColumns);
    const TVector<TString> keyFields = GetKeyFields(keyColumns);
    const auto* inputType = Aggregate->GetInput()->Type;
    const auto* outputType = Aggregate->Type;
    const bool scalarAggregationResult = keyColumns.empty();
    const bool distinctAll = Aggregate->DistinctAll;

    // The difference from the input column is that the agg columns are renamed to columns that do not have the same names for the key column and the input
    // columns.
    TVector<TString> inputFields;
    TVector<TPhysicalAggregationTraits> phyAggregationTraitsList;
    THashMap<TString, TString> renameMap;
    BuildPhysicalAggregationTraits(inputColumns, keyFields, aggregationTraitsList, inputFields, phyAggregationTraitsList, renameMap, inputType, outputType);

    // clang-format off
    input = Ctx.Builder(Pos)
        .Callable("ToFlow")
            .Add(0, input)
        .Seal()
    .Build();
    // clang-format on

    // clang-format off
    auto wideCombiner = Ctx.Builder(Pos)
        .Callable(PhysicalAggregationName)
            .Add(0, NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputColumns, Ctx))
            .Add(1, Ctx.NewAtom(Pos, ""))
            .Add(2, BuildKeyExtractorLambda(keyFields, inputColumns))
            .Add(3, BuildInitHandlerLambda(keyFields, inputFields, phyAggregationTraitsList))
            .Add(4, BuildUpdateHandlerLambda(keyFields, inputFields, phyAggregationTraitsList))
            .Add(5, BuildFinishHandlerLambda(keyFields, phyAggregationTraitsList, distinctAll))
        .Seal()
    .Build();
    // clang-format on

    auto physicalAggregation = BuildNarrowMapForPhysicalAggregationOutput(wideCombiner, keyFields, phyAggregationTraitsList, renameMap, distinctAll);

    // For scalar aggregation result we need to wrap it with Condense.
    if (scalarAggregationResult) {
        physicalAggregation = BuildCondenseForAggregationOutputWithEmptyKeys(physicalAggregation, phyAggregationTraitsList, renameMap, outputType);
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical aggregation] " << KqpExprToPrettyString(TExprBase(physicalAggregation), Ctx);
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("FromFlow")
            .Add(0, physicalAggregation)
        .Seal()
    .Build();
    // clang-format on
}
