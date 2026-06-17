#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {
using namespace NYql::NNodes;

bool IsSuitableToExpandDistinctAggregation(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Aggregate) {
        return false;
    }

    const auto& aggTraitsList = CastOperator<TOpAggregate>(input)->GetAggregationTraits();
    return std::any_of(aggTraitsList.begin(), aggTraitsList.end(), [](const TOpAggregationTraits& aggTraits) { return aggTraits.Distinct; });
}

std::pair<TString, TString> GetAggFunctions(const TString& aggFunc) {
    if (aggFunc == "min" || aggFunc == "max" || aggFunc == "sum" || aggFunc == "avg" || aggFunc == "variance_1_1") {
        return std::make_pair(aggFunc, aggFunc);
    }
    if (aggFunc == "count") {
        return std::make_pair("count", "sum");
    }
    Y_ENSURE(false, "Aggregation function is not supported for splitting.");
}

TIntrusivePtr<IOperator> ExpandSingleDistinct(const TIntrusivePtr<TOpAggregate>& aggregate) {
    const auto& aggTraits = aggregate->GetAggregationTraits().front();
    TVector<TInfoUnit> distinctKeys = aggregate->GetKeyColumns();
    const auto pos = aggregate->Pos;

    // Split into distinct and original aggregation.
    TVector<TOpAggregationTraits> distinctTraitsList;
    for (const auto& key : distinctKeys) {
        distinctTraitsList.emplace_back(TOpAggregationTraits(key, "distinct", key));
    }
    distinctTraitsList.emplace_back(TOpAggregationTraits(aggTraits.OriginalColName, "distinct", aggTraits.OriginalColName));
    distinctKeys.emplace_back(aggTraits.OriginalColName);

    const TIntrusivePtr<IOperator> distinctAggregation =
        MakeIntrusive<TOpAggregate>(aggregate->GetInput(), distinctTraitsList, distinctKeys, EOpPhase::Undefined,
                                    /*distinctAll=*/true, pos);
    TOpAggregationTraits aggregationTraits = aggTraits;
    aggregationTraits.Distinct = false;
    const TVector<TOpAggregationTraits> newAggTraitsList{aggregationTraits};
    return MakeIntrusive<TOpAggregate>(distinctAggregation, newAggTraitsList, aggregate->GetKeyColumns(), EOpPhase::Undefined, /*distinctAll=*/false, pos);
}

TIntrusivePtr<IOperator> BuildDistinct(const TIntrusivePtr<IOperator>& input, TVector<TInfoUnit>&& distColumns) {
    TVector<TOpAggregationTraits> distAggTraitsList;
    for (const auto& distColumn : distColumns) {
        distAggTraitsList.emplace_back(TOpAggregationTraits(distColumn, "distinct", distColumn));
    }
    return MakeIntrusive<TOpAggregate>(input, distAggTraitsList, distColumns, EOpPhase::Undefined, /*distinctAll=*/true, input->Pos);
}

const TTypeAnnotationNode* GetAggregationType(const TTypeAnnotationNode* inputType, const TString& aggFunction, TExprContext& ctx) {
    Y_ENSURE(inputType, "Type is nullptr");
    const TTypeAnnotationNode* resultType = inputType;
    TPositionHandle pos;

    if (aggFunction == "count") {
        return ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
    } else if (aggFunction == "sum") {
        Y_ENSURE(GetSumResultType(pos, *inputType, resultType, ctx), "Unsupported type for sum aggregation function");
    } else if (aggFunction == "avg") {
        Y_ENSURE(false, "Avg not supported for multiple distinct.");
    } else if (aggFunction == "variance_1_1") {
        Y_ENSURE(false, "Variacnce not supported for multiple distinct.");
    }

    return resultType;
}

TIntrusivePtr<IOperator> BuildNullMapElementsExceptOneColumn(const TIntrusivePtr<IOperator>& input, const TTypeAnnotationNode* inputType,
                                                             const TVector<TOpAggregationTraits>& aggTraitsList, const TOpAggregationTraits& realAggTraits,
                                                             const TString& prefix, TPlanProps& props, TExprContext& ctx) {
    Y_ENSURE(inputType);
    auto inputStructType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    auto aggTraitsComparator = [](const TOpAggregationTraits& left, const TOpAggregationTraits& right) {
        return left.OriginalColName.GetFullName() == right.OriginalColName.GetFullName() && left.AggFunction == right.AggFunction &&
               left.ResultColName.GetFullName() == right.ResultColName.GetFullName();
    };

    TVector<TMapElement> mapElements;
    TVector<std::pair<TString, TString>> fakeColumns;
    ui32 i = 0;
    for (const auto& aggTraits : aggTraitsList) {
        const auto originalColName = aggTraits.OriginalColName.GetFullName();
        const auto resultColName = aggTraits.ResultColName.GetFullName();
        const auto mapColName = TInfoUnit(prefix + resultColName);
        TMapElement mapElement;
        TExprNode::TPtr columnExpr;
        auto fieldType = inputStructType->FindItemType(originalColName);
        Y_ENSURE(fieldType, "Aggregation column not found in input type:" << resultColName;);
        if (aggTraitsComparator(aggTraits, realAggTraits)) {
            auto arg = ctx.NewArgument(input->Pos, "arg");
            // clang-format off
            auto body = Build<TCoMember>(ctx, input->Pos)
                .Struct(arg)
                .Name<TCoAtom>()
                    .Value(mapColName.GetFullName())
                .Build()
            .Done().Ptr();
            // clang-format on

            // Count unwraps optional.
            if (!fieldType->IsOptionalOrNull() || aggTraits.AggFunction == "count") {
                // clang-format off
                body = Build<TCoJust>(ctx, input->Pos)
                    .Input(body)
                .Done().Ptr();
                // clang-format on
            }

            // clang-format on
            columnExpr = Build<TCoLambda>(ctx, input->Pos).Args({arg}).Body(body).Done().Ptr();
            // clang-format off

            const auto newName = mapColName.GetFullName() + "_" + ToString(i++);
            fakeColumns.emplace_back(newName, mapColName.GetFullName());
        } else {
            if (fieldType->IsOptionalOrNull()) {
                fieldType = fieldType->Cast<TOptionalExprType>()->GetItemType();
            }

            fieldType = GetAggregationType(fieldType, aggTraits.AggFunction, ctx);
            // clang-format off
            columnExpr = Build<TCoLambda>(ctx, input->Pos)
                .Args({"arg"})
                .Body<TCoNothing>()
                    .OptionalType<TCoOptionalType>()
                        .ItemType(ExpandType(input->Pos, *fieldType, ctx))
                    .Build()
                .Build()
            .Done().Ptr();
            // clang-format on
        }
        mapElement = TMapElement(mapColName, TExpression(columnExpr, &ctx, &props));
        mapElements.emplace_back(mapElement);
    }

    for (const auto& fakeColumn : fakeColumns) {
        mapElements.emplace_back(TMapElement(TInfoUnit(fakeColumn.first), TInfoUnit(fakeColumn.second), input->Pos, &ctx, &props, true));
    }

    if (mapElements.empty()) {
        return input;
    }

    return MakeIntrusive<TOpMap>(input, input->Pos, mapElements);
}

bool NeedToUnwrapOptional(const TTypeAnnotationNode* inputType, const TString& aggField, const std::pair<TString, TString>& aggFunctions,
                          const TVector<TInfoUnit>& keys) {
    Y_ENSURE(inputType);
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto fieldType = structType->FindItemType(aggField);
    Y_ENSURE(fieldType, "Aggregation field not found " << aggField);

    if (aggFunctions.first == "count" && aggFunctions.second == "sum") {
        return true;
    }

    if (!keys.empty()) {
        return !fieldType->IsOptionalOrNull();
    }

    return false;
}

TIntrusivePtr<IOperator> ExpandMultiDistinct(const TIntrusivePtr<TOpAggregate>& aggregate, TPlanProps& props, TExprContext& ctx) {
    const auto& aggTraitsList = aggregate->GetAggregationTraits();
    const auto pos = aggregate->Pos;
    const auto intermediateColumnPrefix = "__intermediate_";

    TIntrusivePtr<IOperator> unionAllResult;
    TVector<TOpAggregationTraits> finalAggTraitsList;
    for (const auto& aggTraits : aggTraitsList) {
        auto partialResult = aggregate->GetInput();
        if (aggTraits.Distinct) {
            // Aggregation column + keys.
            TVector<TInfoUnit> distColumns = aggregate->GetKeyColumns();
            distColumns.emplace_back(aggTraits.OriginalColName);
            partialResult = BuildDistinct(partialResult, std::move(distColumns));
        }

        const auto aggFunctions = GetAggFunctions(aggTraits.AggFunction);
        const auto intermediateColName = TInfoUnit(intermediateColumnPrefix + aggTraits.ResultColName.GetFullName());
        const auto partialAggTraits = TOpAggregationTraits(aggTraits.OriginalColName, aggFunctions.first, intermediateColName);
        const auto finalAggTraits = TOpAggregationTraits(
            intermediateColName, aggFunctions.second, aggTraits.ResultColName, false,
            NeedToUnwrapOptional(aggregate->GetInput()->Type, aggTraits.OriginalColName.GetFullName(), aggFunctions, aggregate->KeyColumns));
        TVector<TOpAggregationTraits> partialAggregationTraitsList{partialAggTraits};
        finalAggTraitsList.emplace_back(finalAggTraits);

        partialResult = MakeIntrusive<TOpAggregate>(partialResult, partialAggregationTraitsList, aggregate->GetKeyColumns(), EOpPhase::Intermediate,
                                                    /*distinctAll=*/false, pos);
        partialResult =
            BuildNullMapElementsExceptOneColumn(partialResult, aggregate->GetInput()->Type, aggTraitsList, aggTraits, intermediateColumnPrefix, props, ctx);

        if (unionAllResult) {
            unionAllResult = MakeIntrusive<TOpUnionAll>(unionAllResult, partialResult, aggregate->Pos);
        } else {
            unionAllResult = partialResult;
        }
    }

    return MakeIntrusive<TOpAggregate>(unionAllResult, finalAggTraitsList, aggregate->GetKeyColumns(), EOpPhase::Final, /*distinctAll=*/false, pos);
}

} // anonymous namespace

TIntrusivePtr<IOperator> TExpandDistinctAggregationRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    if (!IsSuitableToExpandDistinctAggregation(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    if (aggregate->GetAggregationTraits().size() == 1) {
        return ExpandSingleDistinct(aggregate);
    }
    return ExpandMultiDistinct(aggregate, props, rboCtx.ExprCtx);
}

} // namespace NKikimr::NKqp
