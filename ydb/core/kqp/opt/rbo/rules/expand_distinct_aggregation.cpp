#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {
using namespace NYql::NNodes;

bool IsSuitableToExpandDistinctAggregation(const TIntrusivePtr<IOperator>& input) {
    if (input->GetKind() != EOperator::Aggregate) {
        return false;
    }

    const auto aggTraitsList = CastOperator<TOpAggregate>(input)->GetAggregationTraits().Items() | std::views::values;
    return std::any_of(aggTraitsList.begin(), aggTraitsList.end(), [](const TOpAggregationTraits& aggTraits) { return aggTraits.Distinct; });
}

std::pair<TString, TString> GetAggFunctions(const TString& aggFunc) {
    if (aggFunc == "min" || aggFunc == "max" || aggFunc == "sum" || aggFunc == "avg" || aggFunc == "variance_1_1" || aggFunc == "some") {
        return std::make_pair(aggFunc, aggFunc);
    }
    if (aggFunc == "count") {
        return std::make_pair("count", "sum");
    }
    Y_ENSURE(false, "Aggregation function is not supported for splitting.");
}

TIntrusivePtr<IOperator> ExpandSingleDistinct(const TIntrusivePtr<TOpAggregate>& aggregate) {
    const auto& [output, aggTraits] = *aggregate->GetAggregationTraits().Items().begin();
    TOrderedIUs<> distinctKeys = aggregate->GetKeyColumns();
    const auto pos = aggregate->Pos;

    // Split into distinct and original aggregation.
    // Group without traits: identity DISTINCT traits would redefine the input IDs.
    distinctKeys.AppendMissing(aggTraits.Input);

    const TIntrusivePtr<IOperator> distinctAggregation =
        MakeIntrusive<TOpAggregate>(aggregate->GetInput(), TAggregationIUs{}, distinctKeys, EOpPhase::Undefined,
                                    /*distinctAll=*/false, pos);
    TOpAggregationTraits aggregationTraits = aggTraits;
    aggregationTraits.Distinct = false;
    const TAggregationIUs newAggTraitsList{{output, aggregationTraits}};
    return MakeIntrusive<TOpAggregate>(distinctAggregation, newAggTraitsList, aggregate->GetKeyColumns(), EOpPhase::Undefined, /*distinctAll=*/false, pos);
}

TIntrusivePtr<IOperator> BuildDistinct(const TIntrusivePtr<IOperator>& input, TOrderedIUs<>&& distColumns) {
    // Group without traits: identity DISTINCT traits would redefine the input IDs.
    return MakeIntrusive<TOpAggregate>(input, TAggregationIUs{}, std::move(distColumns), EOpPhase::Undefined, /*distinctAll=*/false, input->Pos);
}

bool IsDecimalType(const TTypeAnnotationNode* type) {
    const auto features = NUdf::GetDataTypeInfo(RemoveOptionality(*type).Cast<TDataExprType>()->GetSlot()).Features;
    return (features & NUdf::EDataTypeFeatures::DecimalType);
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
        // Early: (counter, sum)
        std::vector<const TTypeAnnotationNode*> tupleTypes;
        if (IsDecimalType(inputType)) {
            auto decimalType = inputType->Cast<TDataExprParamsType>();
            const auto precision = "35";
            const auto scale = TString(decimalType->GetParamTwo());
            tupleTypes = {ctx.MakeType<TDataExprParamsType>(EDataSlot::Decimal, precision, scale), ctx.MakeType<TDataExprType>(EDataSlot::Uint64)};
        } else {
            tupleTypes = {ctx.MakeType<TDataExprType>(EDataSlot::Double), ctx.MakeType<TDataExprType>(EDataSlot::Uint64)};
        }
        return ctx.MakeType<TTupleExprType>(tupleTypes);
    } else if (aggFunction == "variance_1_1") {
        Y_ENSURE(false, "Variacnce not supported for multiple distinct.");
    }

    return resultType;
}

// realAggTraits maps results to partial IDs; mapColumns gets each result's branch column.
TIntrusivePtr<IOperator> BuildMapWithNullElements(const TIntrusivePtr<IOperator>& input, const TTypeAnnotationNode* inputType,
                                                  const TAggregationIUs& aggTraitsList, const TMappedIUs<TInfoUnitId>& realAggTraits,
                                                  TMappedIUs<TInfoUnitId>& mapColumns, TPlanProps& props, TExprContext& ctx) {
    Y_ENSURE(inputType);
    auto inputStructType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TMapIUs mapElements;
    for (const auto output : aggTraitsList.Keys()) {
        const auto& aggTraits = aggTraitsList.At(output);
        const auto originalColName = ctx.GetIndexAsString(aggTraits.Input);
        TExprNode::TPtr columnExpr;
        auto fieldType = inputStructType->FindItemType(originalColName);
        Y_ENSURE(fieldType, "Aggregation column not found in input type:" << output;);
        if (const auto* partialColName = realAggTraits.Find(output)) {
            const bool needsOptionalWrap = !fieldType->IsOptionalOrNull() || aggTraits.AggFunction == "count";
            if (!needsOptionalWrap) {
                mapColumns.Add(output, *partialColName);
                continue;
            }

            auto arg = ctx.NewArgument(input->Pos, "arg");
            // clang-format off
            auto body = Build<TCoMember>(ctx, input->Pos)
                .Struct(arg)
                .Name<TCoAtom>()
                    .Value(ctx.GetIndexAsString(*partialColName))
                .Build()
            .Done().Ptr();
            // clang-format on

            // Count unwraps optional.
            // clang-format off
            body = Build<TCoJust>(ctx, input->Pos)
                .Input(body)
            .Done().Ptr();
            // clang-format on

            // clang-format on
            columnExpr = Build<TCoLambda>(ctx, input->Pos).Args({arg}).Body(body).Done().Ptr();
            // clang-format off
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
        const auto mapColName = props.InfoUnitRegistry.AddGenerated("distinct_padding");
        mapElements.Add(mapColName, TExpression(columnExpr, &ctx, &props));
        mapColumns.Add(output, mapColName);
    }

    if (mapElements.Keys().Empty()) {
        return input;
    }

    return MakeIntrusive<TOpMap>(input, input->Pos, std::move(mapElements));
}

bool NeedToUnwrapOptional(const TTypeAnnotationNode* inputType, const TString& aggField, const std::pair<TString, TString>& aggFunctions,
                          const TOrderedIUs<>& keys) {
    Y_ENSURE(inputType);
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto fieldType = structType->FindItemType(aggField);
    Y_ENSURE(fieldType, "Aggregation field not found " << aggField);

    if (aggFunctions.first == "count" && aggFunctions.second == "sum") {
        return true;
    }

    if (!keys.Items().empty()) {
        return !fieldType->IsOptionalOrNull();
    }

    return false;
}

TIntrusivePtr<IOperator> ExpandMultiDistinct(const TIntrusivePtr<TOpAggregate>& aggregate, TPlanProps& props, TExprContext& ctx) {
    const auto& aggTraitsList = aggregate->GetAggregationTraits();
    const auto pos = aggregate->Pos;
    auto& registry = props.InfoUnitRegistry;

    // The union defines fresh IDs for the keys and the intermediate columns;
    // every partial aggregation adds its own input ID to each union row.
    TSubstitutions keyReplacements;
    TOrderedIUs<> finalKeys;
    TMappedIUs<TUnionInputRow> rows;
    for (const auto key : aggregate->GetKeyColumns().Items()) {
        if (!keyReplacements.Keys().Contains(key)) {
            const auto output = registry.AddCopy(key);
            keyReplacements.Add(key, output);
            rows.Add(output);
        }
        finalKeys.Append(keyReplacements.At(key));
    }
    TMappedIUs<TInfoUnitId> intermediates;
    for (const auto output : aggTraitsList.Keys()) {
        const auto intermediate = registry.AddGenerated("distinct_union");
        intermediates.Add(output, intermediate);
        rows.Add(intermediate);
    }

    // The input is consumed once per partial aggregation, each through its own
    // Replicate port. Non-primary ports expose fresh IDs.
    const auto hub = TReplicate::Create(aggregate->GetInput(), pos, registry);
    TVector<TIntrusivePtr<IOperator>> unionAllInputs;
    const auto addPartialResult = [&](const TUnorderedIUs& outputs, bool isDistinct) {
        const auto port = hub->AddOutput();
        const auto* rebindings = port->IsPrimary() ? nullptr : &port->GetRebindings();
        const auto rebind = [&](TInfoUnitId id) { return rebindings ? *rebindings->Find(id) : id; };
        TOrderedIUs<> keyColumns;
        for (const auto key : aggregate->GetKeyColumns().Items()) {
            keyColumns.Append(rebind(key));
        }

        TIntrusivePtr<IOperator> partialResult = port;
        if (isDistinct) {
            // Aggregation column + keys.
            TOrderedIUs<> distColumns = keyColumns;
            distColumns.AppendMissing(rebind(aggTraitsList.At(*outputs.begin()).Input));
            partialResult = BuildDistinct(partialResult, std::move(distColumns));
        }

        TAggregationIUs partialAggregationTraitsList;
        TMappedIUs<TInfoUnitId> partialColumns;
        for (const auto output : outputs) {
            const auto& aggTraits = aggTraitsList.At(output);
            const auto partialColName = registry.AddGenerated("distinct_partial");
            partialAggregationTraitsList.Add(partialColName, TOpAggregationTraits{rebind(aggTraits.Input), GetAggFunctions(aggTraits.AggFunction).first});
            partialColumns.Add(output, partialColName);
        }
        partialResult = MakeIntrusive<TOpAggregate>(partialResult, partialAggregationTraitsList, keyColumns, EOpPhase::Intermediate,
                                                    /*distinctAll=*/false, pos);
        TMappedIUs<TInfoUnitId> mapColumns;
        partialResult =
            BuildMapWithNullElements(partialResult, aggregate->GetInput()->Type, aggTraitsList, partialColumns, mapColumns, props, ctx);

        for (const auto& [key, output] : keyReplacements.Items()) {
            rows.At(output).Inputs.push_back(rebind(key));
        }
        for (const auto& [output, intermediate] : intermediates.Items()) {
            rows.At(intermediate).Inputs.push_back(mapColumns.At(output));
        }
        unionAllInputs.push_back(partialResult);
    };

    TAggregationIUs finalAggTraitsList;
    TUnorderedIUs aggTraitsListNotDistinct;

    for (const auto output : aggTraitsList.Keys()) {
        const auto& aggTraits = aggTraitsList.At(output);
        const bool isDistinct = aggTraits.Distinct;
        const auto aggFunctions = GetAggFunctions(aggTraits.AggFunction);
        const auto intermediateColName = intermediates.At(output);
        const auto finalAggTraits = TOpAggregationTraits{
            intermediateColName, aggFunctions.second, false,
            NeedToUnwrapOptional(aggregate->GetInput()->Type, TString(ctx.GetIndexAsString(aggTraits.Input)), aggFunctions, aggregate->GetKeyColumns())};

        finalAggTraitsList.Add(output, finalAggTraits);
        if (!isDistinct) {
            // For not distinct we keep traits and will put them in one aggregation.
            aggTraitsListNotDistinct.Add(output);
        } else {
            addPartialResult(TUnorderedIUs{output}, /*isDistinct=*/true);
        }
    }

    if (!aggTraitsListNotDistinct.Empty()) {
        addPartialResult(aggTraitsListNotDistinct, /*isDistinct=*/false);
    }

    TUnionAllIUs columns(TUnionInputPolicy{unionAllInputs.size()});
    for (const auto output : rows.Keys()) {
        columns.Add(output, std::move(rows.At(output)));
    }
    const TIntrusivePtr<IOperator> unionAllResult = MakeIntrusive<TOpUnionAll>(std::move(unionAllInputs), aggregate->Pos, std::move(columns));
    RebindConsumers(*aggregate, keyReplacements, props.Subplans);

    return MakeIntrusive<TOpAggregate>(unionAllResult, finalAggTraitsList, finalKeys, EOpPhase::Final, /*distinctAll=*/false, pos);
}

} // anonymous namespace

bool TExpandDistinctAggregationRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsSuitableToExpandDistinctAggregation(input);
}

TIntrusivePtr<IOperator> TExpandDistinctAggregationRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    if (!IsSuitableToExpandDistinctAggregation(input)) {
        return input;
    }

    const auto aggregate = CastOperator<TOpAggregate>(input);
    if (aggregate->GetAggregationTraits().Items().size() == 1) {
        // Fast path.
        return ExpandSingleDistinct(aggregate);
    }
    return ExpandMultiDistinct(aggregate, props, rboCtx.ExprCtx);
}

} // namespace NKikimr::NKqp
