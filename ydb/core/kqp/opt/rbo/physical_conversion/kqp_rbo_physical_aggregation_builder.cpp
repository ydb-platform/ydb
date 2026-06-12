#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

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

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode) {
    if (Aggregate->GetAggregationPhase() == EOpPhase::Final) {
        return lambdaArg;
    }

    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    // clang-format off
    return Ctx.Builder(Pos)
        .List()
            .Callable(0, "SafeCast")
                .Add(0, lambdaArg)
                .Add(1, dataTypeForAccumulator)
            .Seal()
            .Callable(1, "Uint64")
                .Atom(0, "1")
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::GetDecimalDataType(const TTypeAnnotationNode* typeNode, bool keepOriginalPrecision) const {
    Y_ENSURE(IsDecimalType(typeNode), "Type is not a Decimal");
    const auto decimalType = GetDecimalType(typeNode);
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("DataType")
            .Atom(0, "Decimal")
             // 35 is used for accumulator.
            .Atom(1, keepOriginalPrecision ? decimalType.Precision : "35")
            .Atom(2, decimalType.Scale)
        .Seal()
    .Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::GetDataTypeForAccumulator(const TTypeAnnotationNode* typeNode, bool keepOriginalPrecision) const {
    if (IsDecimalType(typeNode)) {
        return GetDecimalDataType(typeNode, keepOriginalPrecision);
    }

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("DataType")
            .Atom(0, "Double")
        .Seal()
    .Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationInitialStateForOptionalType(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode) {
    if (Aggregate->GetAggregationPhase() == EOpPhase::Final) {
        // Already tuple
        return lambdaArg;
    }

    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArg)
            .Lambda(1)
                .Param("arg")
                .Callable(0, "Just")
                    .List(0)
                        .Callable(0, "SafeCast")
                            .Arg(0, "arg")
                            .Add(1, dataTypeForAccumulator)
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
                        .Add(0, dataTypeForAccumulator)
                        .Callable(1, "DataType")
                            .Atom(0, "Uint64")
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Decimals not supported for variance");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);

    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    // Create a tuple.
    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        // clang-format off
        return Ctx.Builder(Pos)
            .List()
                .Callable(0, "SafeCast")
                    .Add(0, lambdaArg)
                    .Add(1, dataTypeForAccumulator)
                .Seal()
                .Callable(1, "Double")
                    .Atom(0, "1")
                .Seal()
                .Callable(2, "Double")
                    .Atom(0, "0")
                .Seal()
            .Seal().Build();
        // clang-format on
    }

    // Already tuple.
    return lambdaArg;
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationInitialStateOptionalType(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Decimals not supported for variance.");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);

    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        TExprNodeList tupleTypes{dataTypeForAccumulator, dataTypeForAccumulator, dataTypeForAccumulator};
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, lambdaArg)
                .Lambda(1)
                    .Param("init_param")
                    .Callable(0, "Just")
                        .List(0)
                            .Callable(0, "SafeCast")
                                .Arg(0, "init_param")
                                .Add(1, dataTypeForAccumulator)
                            .Seal()
                            .Callable(1, "Double")
                                .Atom(0, "1")
                            .Seal()
                            .Callable(2, "Double")
                                .Atom(0, "0")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Callable(2, "Nothing")
                    .Callable(0, "OptionalType")
                        .Callable(0, "TupleType")
                            .Add(std::move(tupleTypes))
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();
        // clang-format on
    }

    return lambdaArg;
}

TExprNode::TPtr TPhysicalAggregationBuilder::GetDataTypeForSumAggregation(const TTypeAnnotationNode* itemType) const {
    Y_ENSURE(itemType);
    const auto* type = itemType;
    if (itemType->IsOptionalOrNull()) {
        type = itemType->Cast<TOptionalExprType>()->GetItemType();
    }

    Y_ENSURE(type->GetKind() == ETypeAnnotationKind::Data);
    if (IsDecimalType(type)) {
        return GetDecimalDataType(type);
    }

    TString typeName = TString(type->Cast<TDataExprType>()->GetName());
    if (typeName.StartsWith("Int")) {
        typeName = "Int64";
    } else if (typeName.StartsWith("Uint")) {
        typeName = "Uint64";
    }

    // clang-format on
    return Ctx.Builder(Pos)
        .Callable("DataType")
            .Atom(0, typeName)
        .Seal()
    .Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildSumAggregationInitialState(TExprNode::TPtr lambdaArg, const TTypeAnnotationNode* itemType) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("SafeCast")
            .Add(0, lambdaArg)
            .Add(1, GetDataTypeForSumAggregation(itemType))
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

TExprNode::TPtr TPhysicalAggregationBuilder::BuildCountAggregationUpdateState(TExprNode::TPtr lambdaArgState) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Inc")
            .Add(0, lambdaArgState)
    .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateStateForOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField,
                                                                                           const TTypeAnnotationNode* typeNode) {
    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);

    if (Aggregate->GetAggregationPhase() == EOpPhase::Final) {
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
                                        .Callable(1, "SafeCast")
                                            .Callable(0, "Nth")
                                                .Arg(0, "input_col_arg")
                                                .Atom(1, "0")
                                            .Seal()
                                            .Add(1, dataTypeForAccumulator)
                                        .Seal()
                                    .Seal()
                                    .Callable(1, "AggrAdd")
                                        .Callable(0, "Nth")
                                            .Arg(0, "state_col_arg")
                                            .Atom(1, "1")
                                        .Seal()
                                        .Callable(1, "Nth")
                                            .Arg(0, "input_col_arg")
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
                .Add(2, BuildAvgAggregationInitialStateForOptionalType(lambdaArgField, typeNode))
            .Seal().Build();
        // clang-format on
    }

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
                                    .Callable(1, "SafeCast")
                                        .Arg(0, "input_col_arg")
                                        .Add(1, dataTypeForAccumulator)
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
            .Add(2, BuildAvgAggregationInitialStateForOptionalType(lambdaArgField, typeNode))
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField,
                                                                            const TTypeAnnotationNode* typeNode) {
    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    // For the final phase state is tuple.
    if (Aggregate->GetAggregationPhase() == EOpPhase::Final) {
        // clang-format off
        return Ctx.Builder(Pos)
            .List()
                .Callable(0, "AggrAdd")
                    .Callable(0, "Nth")
                        .Add(0, lambdaArgState)
                        .Atom(1, "0")
                    .Seal()
                    .Callable(1, "SafeCast")
                        .Callable(0, "Nth")
                            .Add(0, lambdaArgField)
                            .Atom(1, "0")
                        .Seal()
                        .Add(1, dataTypeForAccumulator)
                    .Seal()
                .Seal()
                .Callable(1, "AggrAdd")
                    .Callable(0, "Nth")
                        .Add(0, lambdaArgState)
                        .Atom(1, "1")
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, lambdaArgField)
                        .Atom(1, "1")
                    .Seal()
                .Seal()
            .Seal().Build();
        // clang-format on
    } else {
        // clang-format off
        return Ctx.Builder(Pos)
            .List()
                .Callable(0, "AggrAdd")
                    .Callable(0, "Nth")
                        .Add(0, lambdaArgState)
                        .Atom(1, "0")
                    .Seal()
                    .Callable(1, "SafeCast")
                        .Add(0, lambdaArgField)
                        .Add(1, dataTypeForAccumulator)
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
}

TExprNode::TPtr TPhysicalAggregationBuilder::GetNth(TExprNode::TPtr input, TString&& offset) {
    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("Nth")
            .Add(0, input)
            .Atom(1, offset)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceUpdateComputeIntermediate(TExprNode::TPtr fieldArg, TExprNode::TPtr prevCounter, TExprNode::TPtr mean,
                                                                                    TExprNode::TPtr aggState, const TTypeAnnotationNode* typeNode) {
    // clang-format off
    auto delta = Ctx.Builder(Pos)
        .Callable("-")
            .Callable(0, "SafeCast")
                .Add(0, fieldArg)
                .Add(1, GetDataTypeForAccumulator(typeNode))
            .Seal()
            .Add(1, mean)
        .Seal().Build();

    auto currentCounter = Ctx.Builder(Pos)
        .Callable("Inc")
            .Add(0, prevCounter)
        .Seal().Build();

    auto newMean = Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Add(0, mean)
            .Callable(1, "/")
                .Add(0, delta)
                .Add(1, currentCounter)
            .Seal()
        .Seal().Build();

    auto newAggState = Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Add(0, aggState)
            .Callable(1, "/")
                .Callable(0, "*")
                    .Callable(0, "*")
                        .Add(0, delta)
                        .Add(1, delta)
                    .Seal()
                    .Add(1, prevCounter)
                .Seal()
                .Add(1, currentCounter)
            .Seal()
        .Seal().Build();

    return Ctx.Builder(Pos)
        .List()
            .Add(0, newMean)
            .Add(1, currentCounter)
            .Add(2, newAggState)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceUpdateComputeFinal(TExprNode::TPtr fieldMean, TExprNode::TPtr fieldPrevCounter,
                                                                             TExprNode::TPtr fieldAggState, TExprNode::TPtr stateMean,
                                                                             TExprNode::TPtr statePrevCounter, TExprNode::TPtr stateAggState) {
    // clang-format off
    auto counter = Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Add(0, fieldPrevCounter)
            .Add(1, statePrevCounter)
        .Seal().Build();

    auto mean = Ctx.Builder(Pos)
        .Callable("-")
            .Add(0, fieldMean)
            .Add(1, stateMean)
        .Seal().Build();

    auto newMean = Ctx.Builder(Pos)
        .Callable("/")
            .Callable(0, "AggrAdd")
                .Callable(0, "*")
                    .Add(0, fieldMean)
                    .Add(1, fieldPrevCounter)
                .Seal()
                .Callable(1, "*")
                    .Add(0, stateMean)
                    .Add(1, statePrevCounter)
                .Seal()
            .Seal()
            .Add(1, counter)
        .Seal().Build();

    auto newState = Ctx.Builder(Pos)
        .Callable("AggrAdd")
            .Callable(0, "AggrAdd")
                .Add(0, fieldAggState)
                .Add(1, stateAggState)
            .Seal()
            .Callable(1, "/")
                .Callable(0, "*")
                    .Callable(0, "*")
                        .Callable(0, "*")
                            .Add(0, mean)
                            .Add(1, mean)
                        .Seal()
                        .Add(1, fieldPrevCounter)
                    .Seal()
                    .Add(1, statePrevCounter)
                .Seal()
                .Add(1, counter)
            .Seal()
        .Seal().Build();

    return Ctx.Builder(Pos)
        .List()
            .Add(0, newMean)
            .Add(1, counter)
            .Add(2, newState)
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationUpdateState(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField,
                                                                                 const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Decimals not supported for variance.");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);

    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        auto mean = GetNth(lambdaArgState, "0");
        auto prevCounter = GetNth(lambdaArgState, "1");
        auto aggState = GetNth(lambdaArgState, "2");

        return BuildVarianceUpdateComputeIntermediate(lambdaArgField, prevCounter, mean, aggState, typeNode);
    }

    auto fieldMean = GetNth(lambdaArgField, "0");
    auto fieldPrevCounter = GetNth(lambdaArgField, "1");
    auto fieldAggState = GetNth(lambdaArgField, "2");
    auto stateMean = GetNth(lambdaArgState, "0");
    auto statePrevCounter = GetNth(lambdaArgState, "1");
    auto stateAggState = GetNth(lambdaArgState, "2");

   return BuildVarianceUpdateComputeFinal(fieldMean, fieldPrevCounter, fieldAggState, stateMean, statePrevCounter, stateAggState);
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationUpdateStateOptionalType(TExprNode::TPtr lambdaArgState, TExprNode::TPtr lambdaArgField,
                                                                                             const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Decimals not supported for variance.");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);

    TExprNode::TPtr dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    auto stateArg = Ctx.NewArgument(Pos, "state_arg");
    auto fieldArg = Ctx.NewArgument(Pos, "field_arg");
    auto stateMean = GetNth(stateArg, "0");
    auto statePrevCounter = GetNth(stateArg, "1");
    auto stateAggState = GetNth(stateArg, "2");

    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        // clang-format off
        auto innerBody = Ctx.Builder(Pos)
            .Callable("Just")
                .Add(0, BuildVarianceUpdateComputeIntermediate(fieldArg, statePrevCounter, stateMean, stateAggState, typeNode))
            .Seal().Build();

        auto innerLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {fieldArg}), std::move(innerBody));

        auto wrappedLambdaArgField = Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, lambdaArgField)
                .Lambda(1)
                    .Param("lambda_arg_field")
                    .Callable(0, "Just")
                        .List(0)
                            .Callable(0, "SafeCast")
                                .Arg(0, "lambda_arg_field")
                                .Add(1, dataTypeForAccumulator)
                            .Seal()
                            .Callable(1, "Double")
                                .Atom(0, "1")
                            .Seal()
                            .Callable(2, "Double")
                                .Atom(0, "0")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Callable(2, "Nothing")
                    .Callable(0, "OptionalType")
                        .Callable(0, "TupleType")
                            .Add({dataTypeForAccumulator, dataTypeForAccumulator, dataTypeForAccumulator})
                        .Seal()
                    .Seal()
                .Seal()
            .Seal().Build();

        auto outerBody = Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, lambdaArgField)
                .Add(1, innerLambda)
                .Callable(2, "Just")
                    .Add(0, stateArg)
                .Seal()
            .Seal().Build();

        auto outerLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), std::move(outerBody));

        return Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, lambdaArgState)
                .Add(1, outerLambda)
                .Add(2, wrappedLambdaArgField)
            .Seal().Build();
        // clang-format on
    }

    auto fieldMean = GetNth(fieldArg, "0");
    auto fieldPrevCounter = GetNth(fieldArg, "1");
    auto fieldAggState = GetNth(fieldArg, "2");

    auto innerBody = Ctx.Builder(Pos)
        .Callable("Just")
            .Add(0, BuildVarianceUpdateComputeFinal(fieldMean, fieldPrevCounter, fieldAggState, stateMean, statePrevCounter, stateAggState))
        .Seal().Build();

    auto innerLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {fieldArg}), std::move(innerBody));

    auto outerBody = Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArgField)
            .Add(1, innerLambda)
            .Add(2, lambdaArgState)
        .Seal().Build();

    auto outerLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), std::move(outerBody));

    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArgState)
            .Add(1, outerLambda)
            .Add(2, lambdaArgField)
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
                .Add(1, GetDataTypeForSumAggregation(itemType))
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishStateForOptionalType(TExprNode::TPtr lambdaArgState, const TTypeAnnotationNode* typeNode) {
    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        return lambdaArgState;
    }

    // Finally we need an original precision.
    auto dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode, /*keepOriginalPrecision=*/true);
    if (IsDecimalType(typeNode)) {
        // Cast to original precision.
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("IfPresent")
                .Add(0, lambdaArgState)
                .Lambda(1)
                    .Param("arg")
                    .Callable(0, "Just")
                        .Callable(0, "SafeCast")
                            .Callable(0, "DecimalDiv")
                                .Callable(0, "Nth")
                                    .Arg(0, "arg")
                                    .Atom(1, "0")
                                .Seal()
                                .Callable(1, "Nth")
                                    .Arg(0, "arg")
                                    .Atom(1, "1")
                                .Seal()
                            .Seal()
                            .Add(1, dataTypeForAccumulator)
                        .Seal()
                    .Seal()
                .Seal()
                .Callable(2, "Nothing")
                    .Callable(0, "OptionalType")
                        .Add(0, dataTypeForAccumulator)
                    .Seal()
                .Seal()
            .Seal().Build();
        // clang-format on
    }

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
                    .Add(0, dataTypeForAccumulator)
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceFinishCompute(TExprNode::TPtr counter, TExprNode::TPtr aggState) {
    // clang-format off
    TExprNode::TPtr sqrt = Ctx.Builder(Pos)
        .Callable("Udf")
            .Atom(0, "Math.Sqrt")
            .Callable(1, "Void").Seal()
            .Callable(2, "VoidType").Seal()
            .Atom(3, "")
            .Callable(4, "CallableType")
                .List(0).Seal()
                .List(1)
                    .Callable(0, "DataType")
                        .Atom(0, "Double")
                    .Seal()
                .Seal()
                .List(2)
                    .Callable(0, "DataType")
                        .Atom(0, "Double")
                    .Seal()
                    .Atom(1, "")
                    .Atom(2, "1")
                .Seal()
            .Seal()
            .Callable(5, "VoidType").Seal()
            .Atom(6, "")
        .Seal().Build();

    return Ctx.Builder(Pos)
        .Callable("Apply")
            .Add(0, sqrt)
            .Callable(1, "/")
                .Add(0, aggState)
                .Callable(1, "Dec")
                    .Add(0, counter)
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationFinishState(TExprNode::TPtr lambdaArgState, const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Variance for decimals is not supported.");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);

    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        return lambdaArgState;
    }

    auto counter = GetNth(lambdaArgState, "1");
    auto aggState = GetNth(lambdaArgState, "2");

    return BuildVarianceFinishCompute(counter, aggState);
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildVarianceAggregationFinishStateOptionalType(TExprNode::TPtr lambdaArgState,
                                                                                             const TTypeAnnotationNode* typeNode) {
    Y_ENSURE(!IsDecimalType(typeNode), "Variance for decimals is not supported.");
    Y_ENSURE(Aggregate->GetAggregationPhase() != EOpPhase::Undefined);
    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        return lambdaArgState;
    }

    auto dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode);
    auto stateArg = Ctx.NewArgument(Pos, "state_arg");
    auto counter = GetNth(stateArg, "1");
    auto aggState = GetNth(stateArg, "2");

    auto apply = BuildVarianceFinishCompute(counter, aggState);

    // clang-format off
    auto body = Ctx.Builder(Pos)
        .Callable("Just")
            .Add(0, apply)
        .Seal().Build();

    auto lambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {stateArg}), std::move(body));

    return Ctx.Builder(Pos)
        .Callable("IfPresent")
            .Add(0, lambdaArgState)
            .Add(1, lambda)
            .Callable(2, "Nothing")
                .Callable(0, "OptionalType")
                    .Add(0, dataTypeForAccumulator)
                .Seal()
            .Seal()
        .Seal().Build();
    // clang-format on
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildAvgAggregationFinishState(TExprNode::TPtr lambdaArgState, const TTypeAnnotationNode* typeNode) {
    if (Aggregate->GetAggregationPhase() == EOpPhase::Intermediate) {
        return lambdaArgState;
    }

    auto dataTypeForAccumulator = GetDataTypeForAccumulator(typeNode, /*isFinishState=*/true);
    if (IsDecimalType(typeNode)) {
        // Convert to original precision.
        // clang-format off
        return Ctx.Builder(Pos)
            .Callable("SafeCast")
                .Callable(0, "DecimalDiv")
                    .Callable(0, "Nth")
                        .Add(0, lambdaArgState)
                        .Atom(1, "0")
                    .Seal()
                    .Callable(1, "Nth")
                        .Add(0, lambdaArgState)
                        .Atom(1, "1")
                    .Seal()
                .Seal()
                .Add(1, dataTypeForAccumulator)
            .Seal()
        .Build();
        // clang-format on
    }

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

// This lambdas initializes initial state for aggregation.
// It has arguments in the following order - keys, inputs.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildInitHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                    const TVector<TPhysicalAggregationTraits>& aggTraitsList) {
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
        const TTypeAnnotationNode* itemType = aggTraits.InputItemType;

        const auto& aggName = aggTraits.AggFieldName;
        auto it = lambdaArgsMap.find(aggName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr initState = lambdaArgs[it->second];

        if (aggFunction == "count") {
            initState = isOptional ? BuildCountAggregationInitialStateForOptionalType(initState) : BuildCountAggregationInitialState();
        } else if (aggFunction == "avg") {
            initState = isOptional ? BuildAvgAggregationInitialStateForOptionalType(initState, itemType) : BuildAvgAggregationInitialState(initState, itemType);
        } else if (aggFunction == "sum") {
            initState = BuildSumAggregationInitialState(initState, itemType);
        } else if (aggFunction == "distinct") {
            continue;
        } else if (aggFunction == "variance_1_1") {
            initState =
                isOptional ? BuildVarianceAggregationInitialStateOptionalType(initState, itemType) : BuildVarianceAggregationInitialState(initState, itemType);
        }
        lambdaResults.push_back(initState);
    }

    // Create a wide lambda - lambda with multiple outputs.
    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

// This lambda performs an aggregation.
// It has arguments in the following order - keys, inputs, states.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildUpdateHandlerLambda(const TVector<TString>& keyFields, const TVector<TString>& inputFields,
                                                                      const TVector<TPhysicalAggregationTraits>& aggTraitsList, bool isDistinct) {
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

    if (!isDistinct) {
        for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
            lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
            lambdaArgsMap.insert({aggTraitsList[i].StateFieldName, lambdaArgsCounter++});
        }
    }

    TVector<TExprNode::TPtr> lambdaResults;
    for (const auto& aggTraits : aggTraitsList) {
        const auto& aggFunction = aggTraits.AggFunc;
        const auto& fieldName = aggTraits.AggFieldName;
        const auto& stateName = aggTraits.StateFieldName;
        const bool isOptional = aggTraits.InputItemType->IsOptionalOrNull();
        const TTypeAnnotationNode* itemType = aggTraits.InputItemType;
        TExprNode::TPtr updateState;

        auto it = lambdaArgsMap.find(fieldName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr lambdaArgField = lambdaArgs[it->second];

        it = lambdaArgsMap.find(stateName);
        Y_ENSURE(it != lambdaArgsMap.end());
        TExprNode::TPtr lambdaArgState = lambdaArgs[it->second];

        if (aggFunction == "count") {
            updateState =
                isOptional ? BuildCountAggregationUpdateStateForOptionalType(lambdaArgState, lambdaArgField) : BuildCountAggregationUpdateState(lambdaArgState);
        } else if (aggFunction == "avg") {
            updateState = isOptional ? BuildAvgAggregationUpdateStateForOptionalType(lambdaArgState, lambdaArgField, itemType)
                                     : BuildAvgAggregationUpdateState(lambdaArgState, lambdaArgField, itemType);
        } else if (aggFunction == "sum") {
            updateState = BuildSumAggregationUpdateState(lambdaArgState, lambdaArgField, aggTraits.InputItemType);
        } else if (aggFunction == "distinct") {
            continue;
        } else if (aggFunction == "variance_1_1") {
            updateState = isOptional ? BuildVarianceAggregationUpdateStateOptionalType(lambdaArgState, lambdaArgField, aggTraits.InputItemType)
                                     : BuildVarianceAggregationUpdateState(lambdaArgState, lambdaArgField, aggTraits.InputItemType);

        } else {
            auto it = AggregationFunctionToAggregationCallable.find(aggFunction);
            Y_ENSURE(it != AggregationFunctionToAggregationCallable.end());
            const auto& physicalAggregationFunctionName = it->second;
            // clang-format off
            updateState = Ctx.Builder(Pos)
                .Callable(physicalAggregationFunctionName)
                    .Add(0, lambdaArgField)
                    .Add(1, lambdaArgState)
                .Seal().Build();
            // clang-format on
        }
        lambdaResults.push_back(updateState);
    }

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

// This lambda returns aggregation result.
// It has arguments in the following order - keys, states.
TExprNode::TPtr TPhysicalAggregationBuilder::BuildFinishHandlerLambda(const TVector<TString>& keyFields,
                                                                      const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                                      bool isDistinct) {
    ui32 lambdaArgsCounter = 0;
    TVector<TExprNode::TPtr> lambdaArgs;
    THashMap<TString, ui32> lambdaArgsMap;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
        lambdaArgsMap.insert({keyFields[i],  lambdaArgsCounter++});
    }

    TVector<TExprNode::TPtr> lambdaResults;
    for (ui32 i = 0; i < keyFields.size(); ++i) {
        const auto it = lambdaArgsMap.find(keyFields[i]);
        lambdaResults.push_back(lambdaArgs[it->second]);
    }

    if (!isDistinct) {
        for (ui32 i = 0; i < aggTraitsList.size(); ++i) {
            lambdaArgs.push_back(Ctx.NewArgument(Pos, "param" + ToString(lambdaArgsCounter)));
            lambdaArgsMap.insert({aggTraitsList[i].StateFieldName, lambdaArgsCounter++});
        }

        for (const auto& aggTraits : aggTraitsList) {
            const auto& aggFunction = aggTraits.AggFunc;
            const auto& stateName = aggTraits.StateFieldName;
            const bool inputIsOptional = aggTraits.InputItemType->IsOptionalOrNull();
            const bool outputIsOptional = aggTraits.OutputItemType->IsOptionalOrNull();
            const TTypeAnnotationNode* typeNode = aggTraits.InputItemType;
            auto it = lambdaArgsMap.find(stateName);
            TExprNode::TPtr finishState = lambdaArgs[it->second];

            if (aggFunction == "avg") {
                finishState = inputIsOptional ? BuildAvgAggregationFinishStateForOptionalType(finishState, typeNode)
                                              : BuildAvgAggregationFinishState(finishState, typeNode);
            } else if (aggFunction == "variance_1_1") {
                finishState = inputIsOptional ? BuildVarianceAggregationFinishStateOptionalType(finishState, typeNode)
                                              : BuildVarianceAggregationFinishState(finishState, typeNode);
            }

            // Input is not optional, but output is optional - wraph with just.
            if (!inputIsOptional && outputIsOptional && (Aggregate->GetAggregationPhase() != EOpPhase::Intermediate)) {
                // clang-format off
                finishState = Build<TCoJust>(Ctx, Pos)
                    .Input(finishState)
                .Done().Ptr();
                // clang-format on
            }
            lambdaResults.push_back(finishState);
        }
    }

    return Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(lambdaArgs)), std::move(lambdaResults));
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildNarrowMapForPhysicalAggregationOutput(TExprNode::TPtr input, const TVector<TString>& keyFields,
                                                                                        const TVector<TPhysicalAggregationTraits>& aggTraitsList,
                                                                                        const THashMap<TString, TString>& renameMap, bool isDistinct,
                                                                                        EOpPhase aggregationPhase) {
    TVector<TString> outputFields = keyFields;
    if (!isDistinct) {
        for (const auto& aggTraits : aggTraitsList) {
            outputFields.push_back(aggTraits.StateFieldName);
        }
    }

    if (keyFields.empty() && aggregationPhase != EOpPhase::Intermediate) {
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

    const auto outputs = NPhysicalConvertionUtils::BuildNameSet(Aggregate->GetOutputIUs());

    // clang-format off
    return Ctx.Builder(Pos)
        .Callable("NarrowMap")
            .Add(0, input)
            .Lambda(1)
                .Params("wide_param", outputFields.size())
                .Callable(0, "AsStruct")
                .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                    ui32 outputIndex = 0;
                    for (ui32 i = 0; i < outputFields.size(); ++i) {
                        // Apply rename.
                        auto fieldName = outputFields[i];
                        auto it = renameMap.find(fieldName);
                        if (it != renameMap.end()) {
                            fieldName = it->second;
                        }
                        if (!outputs.contains(fieldName)) {
                            continue;
                        }
                        parent.List(outputIndex++)
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

TVector<TString> TPhysicalAggregationBuilder::GetInputColumns() const {
    const auto& aggregationTraitsList = Aggregate->GetAggregationTraits();
    const auto& keyColumns = Aggregate->GetKeyColumns();
    TVector<TString> inputColumns;
    THashSet<TString> uniqueNames;
    // We specify the order: keys, aggtraits.
    for (const auto& keyColumn : keyColumns) {
        const auto fullName = keyColumn.GetFullName();
        if (!uniqueNames.count(fullName)) {
            inputColumns.emplace_back(fullName);
            uniqueNames.insert(fullName);
        }
    }
    for (const auto& aggTraits : aggregationTraitsList) {
        const auto fullName = aggTraits.OriginalColName.GetFullName();
        if (!uniqueNames.count(fullName)) {
            inputColumns.emplace_back(fullName);
            uniqueNames.insert(fullName);
        }
    }
    return inputColumns;
}

void TPhysicalAggregationBuilder::BuildPhysicalAggregationTraits(const TVector<TString>& inputColumns, const TVector<TString>& keyFields,
                                                                 TVector<TString>& inputFields, TVector<TPhysicalAggregationTraits>& phyAggTraitsList,
                                                                 THashMap<TString, TString>& renameMap, const TTypeAnnotationNode* inputType,
                                                                 const TTypeAnnotationNode* outputType) {
    const auto& aggregationTraitsList = Aggregate->GetAggregationTraits();
    Y_ENSURE(inputType && outputType);
    const auto inputStructType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const auto outputStructType = outputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    THashMap<TString, TString> inputColumnsToAggFunction;
    if (IsScalarAggregation() && Aggregate->GetInput()->GetKind() == EOperator::Aggregate) {
        const auto& inputAggTraitsList = CastOperator<TOpAggregate>(Aggregate->GetInput())->GetAggregationTraits();
        for (const auto& inputAggTraits : inputAggTraitsList) {
            inputColumnsToAggFunction.insert({inputAggTraits.ResultColName.GetFullName(), inputAggTraits.AggFunction});
        }
    }

    THashMap<TString, TVector<std::tuple<TString, TString, const TTypeAnnotationNode*, const TTypeAnnotationNode*>>> aggColumns;
    for (const auto& aggregationTraits : aggregationTraitsList) {
        const TString originalColName = aggregationTraits.OriginalColName.GetFullName();
        const TString resultColName = aggregationTraits.ResultColName.GetFullName();
        const TTypeAnnotationNode* inputItemType = inputStructType->FindItemType(originalColName);
        const TTypeAnnotationNode* outputItemType = outputStructType->FindItemType(resultColName);
        Y_ENSURE(inputItemType && outputItemType, "Cannot find type for item");
        aggColumns[originalColName].push_back(std::make_tuple(aggregationTraits.AggFunction, resultColName, inputItemType, outputItemType));
    }

    THashMap<TString, TString> aggFieldsMap;
    THashSet<TString> keyColNames;
    keyColNames.insert(keyFields.begin(), keyFields.end());
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

                TPhysicalAggregationTraits phyTraits(inputField, stateName, aggFunction, inputType, outputType);
                if (inputColumnsToAggFunction.contains(originalColName)) {
                    phyTraits.InputAggFunc = inputColumnsToAggFunction[originalColName];
                }
                phyAggTraitsList.emplace_back(std::move(phyTraits));
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

TVector<TString> TPhysicalAggregationBuilder::GetKeyFields() const {
    const auto& keyColumns = Aggregate->GetKeyColumns();
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
                                                               const THashMap<TString, TString>& renameMap, EOpPhase aggregationPhase) {
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
                        auto maybeInputAggFunc = traits[i].InputAggFunc;
                        // count -> intermediate::count() + final::sum()
                        if (aggFunc == "count" || (aggregationPhase == EOpPhase::Final && aggFunc == "sum" && maybeInputAggFunc.has_value() && *maybeInputAggFunc == "count")) {
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
                                                                                            const TTypeAnnotationNode* type, EOpPhase aggregationPhase) {
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

    return MapCondenseOutput(input, traits, renameMap, aggregationPhase);
}

TExprNode::TPtr TPhysicalAggregationBuilder::BuildPhysicalOp(TExprNode::TPtr input, std::optional<i64> memLimit) {
    // We get input columns based on key columns and aggregation traits.
    const TVector<TString> inputColumns = GetInputColumns();
    // Just a full names of key columns.
    const TVector<TString> keyFields = GetKeyFields();
    const auto* inputType = Aggregate->GetInput()->Type;
    const auto* outputType = Aggregate->Type;
    const bool isDistinct = Aggregate->IsDistinctAll();
    const auto aggregationPhase = Aggregate->AggregationPhase;
    TExprNode::TPtr memoryLimit =
        (memLimit.has_value() && aggregationPhase == EOpPhase::Intermediate) ? Ctx.NewAtom(Pos, ToString(*memLimit)) : Ctx.NewAtom(Pos, "");

    // The difference from the input column is that the agg columns are renamed to columns that do not have the same names for the key column and the input
    // columns.
    TVector<TString> inputFields;
    TVector<TPhysicalAggregationTraits> phyAggregationTraitsList;
    THashMap<TString, TString> renameMap;
    // Here we want to create a internal physical aggregation traits.
    BuildPhysicalAggregationTraits(inputColumns, keyFields, inputFields, phyAggregationTraitsList, renameMap, inputType, outputType);

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
            .Add(1, memoryLimit)
            .Add(2, BuildKeyExtractorLambda(keyFields, inputColumns))
            .Add(3, BuildInitHandlerLambda(keyFields, inputFields, phyAggregationTraitsList))
            .Add(4, BuildUpdateHandlerLambda(keyFields, inputFields, phyAggregationTraitsList, isDistinct))
            .Add(5, BuildFinishHandlerLambda(keyFields, phyAggregationTraitsList, isDistinct))
        .Seal()
    .Build();
    // clang-format on

    auto physicalAggregation =
        BuildNarrowMapForPhysicalAggregationOutput(wideCombiner, keyFields, phyAggregationTraitsList, renameMap, isDistinct, aggregationPhase);

    // For scalar aggregation result we need to wrap it with Condense.
    if (IsScalarAggregation() && aggregationPhase != EOpPhase::Intermediate) {
        physicalAggregation =
            BuildCondenseForAggregationOutputWithEmptyKeys(physicalAggregation, phyAggregationTraitsList, renameMap, outputType, aggregationPhase);
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

bool TPhysicalAggregationBuilder::IsDecimalType(const TTypeAnnotationNode* typeNode) const {
    const auto features = NUdf::GetDataTypeInfo(RemoveOptionality(*typeNode).Cast<TDataExprType>()->GetSlot()).Features;
    bool isdecimal = (features & NUdf::EDataTypeFeatures::DecimalType);
    return isdecimal;
}

TPhysicalAggregationBuilder::TDecimalType TPhysicalAggregationBuilder::GetDecimalType(const TTypeAnnotationNode* typeNode) const {
    auto itemType = typeNode;
    if (itemType->IsOptionalOrNull()) {
        itemType = itemType->Cast<TOptionalExprType>()->GetItemType();
    }
    auto dataExprParams = dynamic_cast<const TDataExprParamsType*>(itemType);
    Y_ENSURE(dataExprParams);
    return TDecimalType(TString(dataExprParams->GetParamOne()), TString(dataExprParams->GetParamTwo()));
}

bool TPhysicalAggregationBuilder::IsScalarAggregation() const {
    return Aggregate->GetKeyColumns().empty();
}
