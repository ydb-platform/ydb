#include "kqp_operator.h"
#include "kqp_rbo_utils.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/public/issue/yql_issue_manager.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp {

using TStatus = NYql::IGraphTransformer::TStatus;

namespace {

using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

const THashSet<TString> SupportedAggregationFunctions = {"sum", "min", "max", "count", "distinct", "avg", "variance_1_1", "some"};

std::pair<TString, const TKikimrTableDescription*> ResolveTable(const TExprNode* kqpTableNode, TExprContext& ctx,
    const TString& cluster, const TKikimrTablesData& tablesData)
{
    if (!EnsureCallable(*kqpTableNode, ctx)) {
        return {"", nullptr};
    }

    if (!TKqpTable::Match(kqpTableNode)) {
        ctx.AddError(TIssue(ctx.GetPosition(kqpTableNode->Pos()), TStringBuilder()
            << "Expected " << TKqpTable::CallableName()));
        return {"", nullptr};
    }

    TString tableName{kqpTableNode->Child(TKqpTable::idx_Path)->Content()};

    auto tableDesc = tablesData.EnsureTableExists(cluster, tableName, kqpTableNode->Pos(), ctx);
    return {std::move(tableName), tableDesc};
}

bool IsNeededToUpdateOlapReadType(TExprNode::TPtr lambda) {
    if (!lambda) {
        return false;
    }
    // Only olap projection can change the return type.
    return !!FindNode(lambda, [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TKqpOlapProjections>(node); });
}

void AnnotateLambdaIfNeeded(TExprNode::TPtr& lambda, TRBOContext& ctx) {
    if (lambda->GetTypeAnn()) {
        return;
    }

    ctx.TypeAnnTransformer.Rewind();
    TStatus status(TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);
    // Could we have an infinity loop?
    } while (status == TStatus::Repeat);

    Y_ENSURE(status == TStatus::Ok, "Cannot type annotate lambda in NEW RBO");
}

TStatus ComputeTypes(TIntrusivePtr<TOpRead> read, TRBOContext& ctx, TPlanProps& props) {
    const auto table = ResolveTable(read->TableCallable.Get(), ctx.ExprCtx, ctx.KqpCtx.Cluster, *ctx.KqpCtx.Tables);
    if (!table.second) {
        YQL_CLOG(TRACE, CoreDq) << "Type annotation for Read, did not resolve tablei.";
        return TStatus::Error;
    }

    YQL_ENSURE(table.second->Metadata, "Expected loaded metadata.");
    const auto meta = table.second->Metadata;

    TVector<TCoAtom> columns;
    THashSet<TString> physicalColumns;
    for (const auto id : read->GetColumns()) {
        const auto column = props.InfoUnitRegistry.Get(id).GetColumnName();
        if (physicalColumns.insert(column).second) { // several IDs may fetch one storage column
            columns.push_back(Build<TCoAtom>(ctx.ExprCtx, read->Pos).Value(column).Done());
        }
    }
    const auto columnsList = Build<TCoAtomList>(ctx.ExprCtx, read->Pos).Add(columns).Done();

    const TTypeAnnotationNode* rowType = GetReadTableRowType(ctx.ExprCtx, *ctx.KqpCtx.Tables, ctx.KqpCtx.Cluster,
        table.first, columnsList, ctx.KqpCtx.Config->SystemColumnsEnabled());
    if (!rowType) {
        YQL_CLOG(TRACE, CoreDq) << "Type annotation for Read, did not get row type.";
        return TStatus::Error;
    }

    const auto structType = rowType->Cast<TStructExprType>();
    TVector<const TItemExprType*> newItemTypes;
    for (const auto id : read->GetColumns()) {
        const auto* itemType = structType->FindItemType(props.InfoUnitRegistry.Get(id).GetColumnName());
        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(id), itemType));
    }
    auto newStructType = ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes);

    if (read->OriginalPredicate.has_value()) {
        auto& lambda = read->OriginalPredicate.value().Node;
        if (!UpdateLambdaAllArgumentsTypes(lambda, {newStructType}, ctx.ExprCtx)) {
            YQL_CLOG(TRACE, CoreDq) << "Could not update original filter lambda arg types.";
            return IGraphTransformer::TStatus::Error;
        }

        AnnotateLambdaIfNeeded(lambda, ctx);
        Y_ENSURE(lambda->GetTypeAnn(), "Cannot type annotate original filter lambda.");
    }

    if (IsNeededToUpdateOlapReadType(read->OlapFilterLambda)) {
        auto& lambda = read->OlapFilterLambda;
        if (!UpdateLambdaAllArgumentsTypes(lambda, {ctx.ExprCtx.MakeType<TFlowExprType>(newStructType)}, ctx.ExprCtx)) {
            YQL_CLOG(TRACE, CoreDq) << "Could not update olap filter lambda arg types.";
            return IGraphTransformer::TStatus::Error;
        }

        AnnotateLambdaIfNeeded(lambda, ctx);
        Y_ENSURE(lambda->GetTypeAnn(), "Cannot type annotate olap lambda.");

        // The OLAP program's row schema already uses ID field names.
        newStructType = lambda->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TStructExprType>();
    }

    read->Type = ctx.ExprCtx.MakeType<TListExprType>(newStructType);
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpEmptySource> emptySource, TRBOContext& ctx, TPlanProps& props) {
    TVector<const TItemExprType*> resultItems;
    if (emptySource->Input) {
        const auto* source = emptySource->Input->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto id : emptySource->GetOutputIUs()) {
            const auto* type = source->FindItemType(props.InfoUnitRegistry.Get(id).GetFullName());
            Y_ENSURE(type, "Unknown parameter-table column " << id);
            resultItems.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(id), type));
        }
    }

    auto resultType = ctx.ExprCtx.MakeType<TStructExprType>(resultItems);

    emptySource->Type = ctx.ExprCtx.MakeType<TListExprType>(resultType);

    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpReplicate> output, TRBOContext& ctx) {
    auto& input = *output->GetInput();
    if (output->IsPrimary()) {
        output->Type = input.Type;
        return TStatus::Ok;
    }

    const auto& rebindings = output->GetRebindings();
    const auto* inputType = input.Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> items;
    for (const auto id : input.GetOutputIUs()) {
        const auto* type = inputType->FindItemType(ctx.ExprCtx.GetIndexAsString(id));
        Y_ENSURE(type, "Missing Replicate input type for " << id);
        items.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(*rebindings.Find(id)), type));
    }
    output->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(items));
    return TStatus::Ok;
}

const TStructExprType* AddSubplanTypes(const TStructExprType* itemType, const TUnorderedIUs& subplanContextIUs, TRBOContext& ctx, TPlanProps& props) {
    TVector<const TItemExprType*> structItemTypes;
    for (const auto *item : itemType->GetItems()) {
        structItemTypes.push_back(item);
    }

    for (const auto iu : subplanContextIUs) {
        const TTypeAnnotationNode* subplanType;
        const auto& subplanEntry = props.Subplans.At(iu);
        if (subplanEntry.Type == ESubplanType::EXPR) {
            auto subplan = CastOperator<IOperator>(subplanEntry.Plan);
            Y_ENSURE(subplanEntry.ResultIU, "Scalar subplan has no result binding");
            const auto result = *subplanEntry.ResultIU;
            subplanType = subplan->GetIUType(result, ctx.ExprCtx);
            Y_ENSURE(subplanType, "Cannot infer scalar subplan result type for " << result);
            // For scalar subquery sublan type will always be optional.
            if (!subplanType->IsOptionalOrNull()) {
                subplanType = ctx.ExprCtx.MakeType<TOptionalExprType>(subplanType);
            }
        } else {
            if (!props.PgSyntax) {
                subplanType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Bool);
            } else {
                subplanType = ctx.ExprCtx.MakeType<TPgExprType>(NYql::NPg::LookupType("bool").TypeId);
            }
        }
        auto newType = ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(iu), subplanType);
        structItemTypes.push_back(newType);
    }

    return ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes);
}

TStatus ComputeTypes(TIntrusivePtr<TOpFilter> filter, TRBOContext& ctx, TPlanProps& props) {
    const TTypeAnnotationNode* inputType = filter->GetInput()->Type;
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, inputType: " << *inputType;

    auto itemType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType: " << *(TTypeAnnotationNode*)itemType;

    const auto subplanContextIUs = filter->GetSubplanIUs(props.Subplans);
    if (!subplanContextIUs.Empty()) {
        itemType = AddSubplanTypes(itemType, subplanContextIUs, ctx, props);
    }
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType after scalars: " << *(TTypeAnnotationNode*)itemType;

    auto filterExpression = filter->GetFilterExpression();
    auto lambda = filterExpression.Node;

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.ExprCtx)) {
        YQL_CLOG(TRACE, CoreDq) << "Could not update lambda arg types";
        return IGraphTransformer::TStatus::Error;
    }

    AnnotateLambdaIfNeeded(lambda, ctx);

    const TTypeAnnotationNode* lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        YQL_CLOG(TRACE, CoreDq) << "Could not infer lambda types";
        return IGraphTransformer::TStatus::Error;
    }

    if (!IsDataOrOptionalOfDataOrPg(lambdaType)) {
        ctx.ExprCtx.AddError(TIssue(ctx.ExprCtx.GetPosition(filter->Pos), TStringBuilder() << "Expected data or pg type, but got " << *lambdaType));
        return IGraphTransformer::TStatus::Error;
    }

    lambdaType = RemoveOptionalType(lambdaType);

    const TPgExprType* pgType = nullptr;
    if (IsPg(lambdaType, pgType)) {
        if (pgType->GetName() != "bool") {
            ctx.ExprCtx.AddError(TIssue(ctx.ExprCtx.GetPosition(filter->Pos), TStringBuilder() << "Expected pgbool type, but got " << *lambdaType));
            return IGraphTransformer::TStatus::Error;
        }
    }

    else if(!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.ExprCtx, true)) {
        return IGraphTransformer::TStatus::Error;
    }

    filter->SetFilterExpression(TExpression(std::move(lambda), filterExpression.Ctx, filterExpression.PlanProps));
    filter->Type = inputType;

    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpMap> map, TRBOContext& ctx, TPlanProps& props) {
    const TTypeAnnotationNode* inputType = map->GetInput()->Type;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    // Logical Maps are append-only: every input column passes through.
    TVector<const TItemExprType*> resStructItemTypes = structType->GetItems();

    for (const auto& [output, mapElement] : map->GetMapElements().Items()) {
        // This is type annotation update inplace, which is different comparing to yql type annotation.
        auto expression = mapElement.GetExpression();
        auto lambda = expression.Node;

        auto subplanContextIUs = expression.GetInputIUs(true,false);
        subplanContextIUs.IntersectWith(props.Subplans.Bindings());

        auto currStructType = structType;
        if (!subplanContextIUs.Empty()) {
            currStructType = AddSubplanTypes(currStructType, subplanContextIUs, ctx, props);
        }

        if (!UpdateLambdaAllArgumentsTypes(lambda, {currStructType}, ctx.ExprCtx)) {
            return IGraphTransformer::TStatus::Error;
        }

        AnnotateLambdaIfNeeded(lambda, ctx);

        const TTypeAnnotationNode* lambdaType = lambda->GetTypeAnn();
        Y_ENSURE(lambdaType);
        auto mapLambdaType = ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output), lambdaType);
        resStructItemTypes.push_back(mapLambdaType);
        if (lambda != expression.Node) {
            map->SetMapElementExpression(output, TExpression(std::move(lambda), expression.Ctx, expression.PlanProps));
        }
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resStructItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);
    map->Type = resultAnn;
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Map done: " << *resultAnn;

    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpAddDependencies> addDeps, TRBOContext& ctx) {
    const TTypeAnnotationNode* inputType = addDeps->GetInput()->Type;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto resStructItemTypes = structType->GetItems();

    for (const auto& [local, capture] : addDeps->GetDependencies().Items()) {
        resStructItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(local), capture.Type));
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resStructItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);
    addDeps->Type = resultAnn;
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpUnionAll> unionAll, TRBOContext& ctx) {
    TVector<const TStructExprType*> inputStructTypes;
    inputStructTypes.reserve(unionAll->GetChildren().size());
    for (const auto& input : unionAll->GetChildren()) {
        inputStructTypes.push_back(input->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>());
    }

    // The output type of every column is taken from the first input.
    TVector<const TItemExprType*> resultItems;
    resultItems.reserve(unionAll->GetColumns().Items().size());
    for (const auto& [output, column] : unionAll->GetColumns().Items()) {
        const TTypeAnnotationNode* resultType = nullptr;
        for (size_t i = 0; i < inputStructTypes.size(); ++i) {
            const auto* inputType = inputStructTypes[i]->FindItemType(ctx.ExprCtx.GetIndexAsString(column.Inputs[i]));
            Y_ENSURE(inputType, "Missing UnionAll source type for input " << i << ": " << column.Inputs[i]);

            // FIXME: This currently does not pass after UnionAll semantic update
            // Y_ENSURE(!resultType || IsSameAnnotation(*resultType, *inputType),
            //     "UnionAll source type mismatch for output " << output);

            if (!resultType) {
                resultType = inputType;
            }
        }

        resultItems.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output), resultType));
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resultItems);
    unionAll->Type = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpAggregate> aggregate, TRBOContext& ctx) {
    auto inputType = aggregate->GetInput()->Type;
    const auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    const bool scalarAggregation = aggregate->GetKeyColumns().Items().empty();
    TPositionHandle pos = aggregate->Pos;
    const auto aggregationPhase = aggregate->GetAggregationPhase();

    TVector<const TItemExprType*> newItemTypes;
    THashMap<TString, const TTypeAnnotationNode*> aggTraitsMap;
    for (const auto itemType : structType->GetItems()) {
        const auto itemName = itemType->GetName();
        aggTraitsMap.emplace(itemName, itemType->GetItemType());
    }

    if (!aggregate->IsDistinctAll()) {
        for (const auto keyColumn : aggregate->GetKeyColumns().Items()) {
            const auto it = aggTraitsMap.find(ctx.ExprCtx.GetIndexAsString(keyColumn));
            Y_ENSURE(it != aggTraitsMap.end());
            newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(it->first, it->second));
        }
    }

    // In case type annotation is running for final aggregation.
    // (resultColName, (aggFunction, isOptional)).
    THashMap<TString, std::pair<TString, bool>> intermediateAggregation;
    if (aggregate->GetInput()->GetKind() == EOperator::Aggregate && aggregationPhase == EOpPhase::Final) {
        const auto& aggTraitsList = CastOperator<TOpAggregate>(aggregate->GetInput())->GetAggregationTraits();
        for (const auto& [result, aggTraits] : aggTraitsList.Items()) {
            const auto resultColName = TString(ctx.ExprCtx.GetIndexAsString(result));
            const auto& aggFunc = aggTraits.AggFunction;
            const auto itemType = structType->FindItemType(resultColName);
            Y_ENSURE(itemType, "Cannot find field name in input type.");
            intermediateAggregation.emplace(resultColName, std::make_pair(aggFunc, itemType->IsOptionalOrNull()));
        }
    }

    for (const auto& [result, traits] : aggregate->GetAggregationTraits().Items()) {
        const auto originalColName = TString(ctx.ExprCtx.GetIndexAsString(traits.Input));
        const auto& aggFunction = traits.AggFunction;
        Y_ENSURE(SupportedAggregationFunctions.contains(aggFunction), TStringBuilder() << "Unsupported aggregation function: " << aggFunction;);
        const auto resultColName = ctx.ExprCtx.GetIndexAsString(result);
        const auto it = aggTraitsMap.find(originalColName);
        Y_ENSURE(it != aggTraitsMap.end(), "Cannot find aggregation input " << originalColName
            << " in input type " << *inputType);
        auto aggFieldType = it->second;

        if (aggFunction == "count") {
            aggFieldType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
        } else if (aggFunction == "sum") {
            Y_ENSURE(GetSumResultType(pos, *it->second, aggFieldType, ctx.ExprCtx), "Unsupported type for sum aggregation function");
        } else if (aggFunction == "avg") {
            if (auto unwrappedType = &RemoveOptionality(*aggFieldType); unwrappedType->GetKind() != ETypeAnnotationKind::Tuple) {
                Y_ENSURE(GetAvgResultType(pos, *it->second, aggFieldType, ctx.ExprCtx), "Unsupported type for avg aggregation function");
            }
        } else if (aggFunction == "variance_1_1") {
            Y_ENSURE(GetAvgResultType(pos, *it->second, aggFieldType, ctx.ExprCtx), "Unsupported type for variance aggregation function");
        }

        // Special case for scalar aggregation (aka aggregation with empty keys).
        if (aggregationPhase != EOpPhase::Intermediate && scalarAggregation && !aggFieldType->IsOptionalOrNull() &&
            (aggFunction == "min" || aggFunction == "max" || aggFunction == "sum" || aggFunction == "avg" || aggFunction == "variance_1_1" ||
             aggFunction == "some")) {
            const auto it = intermediateAggregation.find(originalColName);
            // count -> count::intermediate + sum::final
            if ((it == intermediateAggregation.end()) || (it->second.first != "count")) {
                aggFieldType = ctx.ExprCtx.MakeType<TOptionalExprType>(aggFieldType);
            }
        }

        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(resultColName, aggFieldType));
    }

    aggregate->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpGroupingSets> groupingSets, TRBOContext& ctx) {
    const auto& aggregate = CastOperator<TOpAggregate>(*groupingSets->GetInput());
    const auto* structType = aggregate.Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TUnorderedIUs keysPresentInEverySet;
    bool first = true;
    bool hasEmptySet = false;
    for (const auto& groupingSet : groupingSets->GetGroupingSets()) {
        hasEmptySet = hasEmptySet || groupingSet.Empty();
        if (first) {
            keysPresentInEverySet = groupingSet;
            first = false;
        } else {
            keysPresentInEverySet.IntersectWith(groupingSet);
        }
    }

    const auto& keyColumns = aggregate.GetKeyColumns().Unordered();
    TUnorderedIUs scalarOptionalResults;
    if (hasEmptySet) {
        for (const auto& [result, traits] : aggregate.GetAggregationTraits().Items()) {
            if (traits.AggFunction == "min" || traits.AggFunction == "max" || traits.AggFunction == "sum" || traits.AggFunction == "avg" ||
                traits.AggFunction == "variance_1_1" || traits.AggFunction == "some") {
                scalarOptionalResults.Add(result);
            }
        }
    }

    TVector<const TItemExprType*> resultItems;
    resultItems.reserve(groupingSets->GetOutputIUs().Size());
    for (const auto& [output, iu] : groupingSets->GetColumns().Items()) {
        const auto* itemType = structType->FindItemType(ctx.ExprCtx.GetIndexAsString(iu));
        const bool nonCommonKey = keyColumns.Contains(iu) && !keysPresentInEverySet.Contains(iu);
        if ((nonCommonKey || scalarOptionalResults.Contains(iu)) && !itemType->IsOptionalOrNull()) {
            itemType = ctx.ExprCtx.MakeType<TOptionalExprType>(itemType);
        }
        resultItems.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output), itemType));
    }

    for (const auto output : groupingSets->GetGroupingIndicators().Keys()) {
        resultItems.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output),
                                                                        ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)));
    }
    groupingSets->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(resultItems));
    return TStatus::Ok;
}

TVector<const TItemExprType*> AddOptional(const TVector<const TItemExprType*>& types, TRBOContext& rboCtx) {
    auto& ctx = rboCtx.ExprCtx;
    TVector<const TItemExprType*> optionalTypes;
    for (ui32 i = 0, e = types.size(); i < e; ++i) {
        const auto itemType = types[i]->GetItemType();
        if (!itemType->IsOptionalOrNull()) {
            optionalTypes.push_back(ctx.MakeType<TItemExprType>(types[i]->GetName(), ctx.MakeType<TOptionalExprType>(itemType)));
        } else {
            optionalTypes.push_back(types[i]);
        }
    }
    return optionalTypes;
}

TStatus ComputeTypes(TIntrusivePtr<TOpJoin> join, TRBOContext& ctx) {
    auto leftInputType = join->GetLeftInput()->Type;
    auto rightInputType = join->GetRightInput()->Type;

    auto leftItemType = leftInputType->Cast<TListExprType>()->GetItemType();
    auto rightItemType = rightInputType->Cast<TListExprType>()->GetItemType();

    TVector<const TItemExprType*> structItemTypes;
    TVector<const TItemExprType*> crossProductTypes;
    TVector<const TItemExprType*> leftItemTypes = leftItemType->Cast<TStructExprType>()->GetItems();
    TVector<const TItemExprType*> rightItemTypes = rightItemType->Cast<TStructExprType>()->GetItems();

    // Build a cross-product type to annotate join filters
    crossProductTypes.insert(crossProductTypes.end(), leftItemTypes.begin(), leftItemTypes.end());
    crossProductTypes.insert(crossProductTypes.end(), rightItemTypes.begin(), rightItemTypes.end());
    auto crossProductType = ctx.ExprCtx.MakeType<TStructExprType>(crossProductTypes);

    for (auto& filterExpr : join->JoinFilters) {
        auto& lambda = filterExpr.Node;

        if (!UpdateLambdaAllArgumentsTypes(lambda, {crossProductType}, ctx.ExprCtx)) {
            YQL_CLOG(TRACE, CoreDq) << "Could not update lambda arg types";
            return IGraphTransformer::TStatus::Error;
        }

        AnnotateLambdaIfNeeded(lambda, ctx);
    }

    if (!JoinOutputsRight(join->JoinKind)) {
        rightItemTypes = {};
    } else if (!JoinOutputsLeft(join->JoinKind)) {
        leftItemTypes = {};
    } else if (join->JoinKind == "Left") {
        rightItemTypes = AddOptional(rightItemTypes, ctx);
    } else if (join->JoinKind == "Right") {
        leftItemTypes = AddOptional(leftItemTypes, ctx);
    } else if (join->JoinKind == "Full") {
        leftItemTypes = AddOptional(leftItemTypes, ctx);
        rightItemTypes = AddOptional(rightItemTypes, ctx);
    }

    structItemTypes.insert(structItemTypes.end(), leftItemTypes.begin(), leftItemTypes.end());
    structItemTypes.insert(structItemTypes.end(), rightItemTypes.begin(), rightItemTypes.end());

    auto resultStructType = ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultStructType);
    join->Type = resultAnn;

    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpDependentJoin> dependentJoin, TRBOContext& ctx) {
    const auto* domainItemType = dependentJoin->GetDomain()->Type->Cast<TListExprType>()->GetItemType();
    const auto* inputItemType = dependentJoin->GetInput()->Type->Cast<TListExprType>()->GetItemType();

    TVector<const TItemExprType*> structItemTypes = domainItemType->Cast<TStructExprType>()->GetItems();
    THashSet<TStringBuf> domainNames;
    for (const auto* item : structItemTypes) {
        domainNames.insert(item->GetName());
    }

    for (const auto* item : inputItemType->Cast<TStructExprType>()->GetItems()) {
        if (!domainNames.contains(item->GetName())) {
            structItemTypes.push_back(item);
        }
    }

    dependentJoin->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpLimit> limit, TRBOContext& ctx) {
    auto inputType = limit->GetInput()->Type;
    const auto* structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    auto& lambda = limit->LimitCond.Node;
    if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
        return IGraphTransformer::TStatus::Error;
    }

    AnnotateLambdaIfNeeded(lambda, ctx);

    // TODO: Add sanity checks.
    limit->Type = inputType;
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpSort> sort, TRBOContext& ctx) {
    Y_UNUSED(ctx);
    auto inputType = sort->GetInput()->Type;
    // TODO: Add sanity checks.
    sort->Type = inputType;
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpWindow> window, TRBOContext& ctx) {
    const auto* inputType = window->GetInput()->Type;
    const auto* structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TVector<const TItemExprType*> itemTypes(structType->GetItems().begin(), structType->GetItems().end());
    for (const auto& [output, func] : window->GetWindowFuncs().Items()) {
        const TTypeAnnotationNode* resultType = nullptr;
        if (func.Kind == EWindowFuncKind::Native) {
            resultType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
        } else {
            Y_ENSURE(func.Arguments.Items().size() == 1, "Window aggregate " << func.Function << " expects a single argument");
            const auto* argType = structType->FindItemType(ctx.ExprCtx.GetIndexAsString(func.Arguments.Items()[0]));
            Y_ENSURE(argType, "Unknown window function argument " << func.Arguments.Items()[0]);

            if (func.Function == "count") {
                itemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output),
                                                                        ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64)));
                continue;
            }
            if (func.Function == "sum") {
                Y_ENSURE(GetSumResultType(window->Pos, *argType, resultType, ctx.ExprCtx), "Unsupported type for sum over a window");
            } else if (func.Function == "avg" || func.Function == "variance_1_1") {
                Y_ENSURE(GetAvgResultType(window->Pos, *argType, resultType, ctx.ExprCtx), "Unsupported type for avg over a window");
            } else {
                resultType = argType;
            }
            if (!resultType->IsOptionalOrNull()) {
                resultType = ctx.ExprCtx.MakeType<TOptionalExprType>(resultType);
            }
        }
        itemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(output), resultType));
    }

    window->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(itemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpTableLookup> lookup, TRBOContext& ctx, TPlanProps& props) {
    const auto table = ResolveTable(lookup->Table.Get(), ctx.ExprCtx, ctx.KqpCtx.Cluster, *ctx.KqpCtx.Tables);
    if (!table.second) {
        return TStatus::Error;
    }

    TVector<TCoAtom> columns;
    THashSet<TString> physicalColumns;
    for (const auto id : lookup->GetColumns()) {
        const auto column = props.InfoUnitRegistry.Get(id).GetColumnName();
        if (physicalColumns.insert(column).second) {
            columns.push_back(Build<TCoAtom>(ctx.ExprCtx, lookup->Pos).Value(column).Done());
        }
    }
    const auto columnsList = Build<TCoAtomList>(ctx.ExprCtx, lookup->Pos).Add(columns).Done();

    const TTypeAnnotationNode* rowType = GetReadTableRowType(ctx.ExprCtx, *ctx.KqpCtx.Tables, ctx.KqpCtx.Cluster,
        table.first, columnsList, ctx.KqpCtx.Config->SystemColumnsEnabled());
    if (!rowType) {
        return TStatus::Error;
    }

    TVector<const TItemExprType*> newItemTypes;
    for (const auto id : lookup->GetColumns()) {
        const auto* itemType = rowType->Cast<TStructExprType>()->FindItemType(props.InfoUnitRegistry.Get(id).GetColumnName());
        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(ctx.ExprCtx.GetIndexAsString(id), itemType));
    }
    auto newStructType = ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes);

    if (!lookup->IsJoin()) {
        lookup->Type = ctx.ExprCtx.MakeType<TListExprType>(newStructType);
        return TStatus::Ok;
    }

    if (lookup->FetchedRowFilter) {
        auto& lambda = lookup->FetchedRowFilter->Node;
        if (!UpdateLambdaAllArgumentsTypes(lambda, {newStructType}, ctx.ExprCtx)) {
            YQL_CLOG(TRACE, CoreDq) << "Could not update the lookup join filter lambda arg types";
            return TStatus::Error;
        }

        AnnotateLambdaIfNeeded(lambda, ctx);
        if (!lambda->GetTypeAnn()) {
            YQL_CLOG(TRACE, CoreDq) << "Could not infer the lookup join filter lambda type";
            return TStatus::Error;
        }
        if (!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.ExprCtx, true)) {
            return TStatus::Error;
        }
    }

    const auto* leftItemType = lookup->GetInput()->Type->Cast<TListExprType>()->GetItemType();
    if (!EnsureStructType(lookup->Pos, *leftItemType, ctx.ExprCtx)) {
        return TStatus::Error;
    }

    TVector<const TTypeAnnotationNode*> tupleItemTypes;
    tupleItemTypes.push_back(leftItemType);
    tupleItemTypes.push_back(ctx.ExprCtx.MakeType<TOptionalExprType>(newStructType));
    tupleItemTypes.push_back(ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64));
    lookup->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TTupleExprType>(tupleItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpIndexLookupJoin> lookupJoin, TRBOContext& ctx) {
    const auto* itemType = lookupJoin->GetInput()->Type->Cast<TListExprType>()->GetItemType();
    if (!EnsureTupleType(lookupJoin->Pos, *itemType, ctx.ExprCtx)) {
        return TStatus::Error;
    }

    const auto* tupleType = itemType->Cast<TTupleExprType>();
    // (left row, optional(right row), cookie)
    Y_ENSURE(tupleType->GetSize() == 3, "Unexpected lookup join input tuple");
    const auto leftItemTypes = tupleType->GetItems()[0]->Cast<TStructExprType>()->GetItems();

    TVector<const TItemExprType*> structItemTypes;
    structItemTypes.insert(structItemTypes.end(), leftItemTypes.begin(), leftItemTypes.end());

    if (JoinOutputsRight(lookupJoin->JoinKind)) {
        auto rightItemTypes = tupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems();
        // An unmatched left row of a left join produces NULLs on the right side.
        if (lookupJoin->JoinKind == "Left") {
            rightItemTypes = AddOptional(rightItemTypes, ctx);
        }
        structItemTypes.insert(structItemTypes.end(), rightItemTypes.begin(), rightItemTypes.end());
    }

    lookupJoin->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<IOperator> op, TRBOContext& ctx, TPlanProps& props);

TStatus ComputeTypes(TIntrusivePtr<TOpCBOTree> cboTree, TRBOContext& ctx, TPlanProps& props) {
    for (auto op : cboTree->TreeNodes) {
        if (auto status = ComputeTypes(op, ctx, props); status != TStatus::Ok) {
            return status;
        }
    }
    cboTree->Type = cboTree->TreeRoot->Type;
    return TStatus::Ok;
}

TStatus ComputeTypes(TIntrusivePtr<TOpTableEffect> tableEffect, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    tableEffect->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TResourceExprType>(KqpEffectTag));
    return TStatus::Ok;
}

// Row fields are named by decimal IDs; map the IDs visible here to labels.
void AppendIdLegend(TStringBuilder& message, IOperator& op, TPlanProps& props) {
    TUnorderedIUs ius = op.GetOutputIUs();
    for (const auto& child : op.GetChildren()) {
        ius.UnionWith(child->GetOutputIUs());
    }
    for (const auto id : op.GetSubplanIUs(props.Subplans)) {
        ius.Add(id);
    }
    if (!ius.Empty()) {
        message << "; IU bindings:";
        for (const auto id : ius) {
            message << " " << props.InfoUnitRegistry.GetDebugName(id) << ";";
        }
    }
}

TStatus ComputeTypes(TIntrusivePtr<IOperator> op, TRBOContext& ctx, TPlanProps& props) {
    // Only collect bindings and render registry names if YQL emits an issue.
    TIssueScopeGuard scope(ctx.ExprCtx.IssueManager, [&] {
        TStringBuilder message;
        message << "While annotating RBO " << op->GetExplainName();
        try {
            AppendIdLegend(message, *op, props);
        } catch (...) {
        }
        return MakeIntrusive<TIssue>(ctx.ExprCtx.GetPosition(op->Pos), message);
    });

    if (MatchOperator<TOpEmptySource>(op)) {
        return ComputeTypes(CastOperator<TOpEmptySource>(op), ctx, props);
    }
    else if (MatchOperator<TOpRead>(op)) {
        return ComputeTypes(CastOperator<TOpRead>(op), ctx, props);
    }
    else if (MatchOperator<TOpReplicate>(op)) {
        return ComputeTypes(CastOperator<TOpReplicate>(op), ctx);
    }
    else if(MatchOperator<TOpFilter>(op)) {
        return ComputeTypes(CastOperator<TOpFilter>(op), ctx, props);
    }
    else if(MatchOperator<TOpMap>(op)) {
        return ComputeTypes(CastOperator<TOpMap>(op), ctx, props);
    }
    else if(MatchOperator<TOpAddDependencies>(op)) {
        return ComputeTypes(CastOperator<TOpAddDependencies>(op), ctx);
    }
    else if(MatchOperator<TOpJoin>(op)) {
        return ComputeTypes(CastOperator<TOpJoin>(op), ctx);
    }
    else if(MatchOperator<TOpDependentJoin>(op)) {
        return ComputeTypes(CastOperator<TOpDependentJoin>(op), ctx);
    }
    else if(MatchOperator<TOpUnionAll>(op)) {
        return ComputeTypes(CastOperator<TOpUnionAll>(op), ctx);
    }
    else if(MatchOperator<TOpLimit>(op)) {
        return ComputeTypes(CastOperator<TOpLimit>(op), ctx);
    }
    else if (MatchOperator<TOpSort>(op)) {
        return ComputeTypes(CastOperator<TOpSort>(op), ctx);
    }
    else if (MatchOperator<TOpTableLookup>(op)) {
        return ComputeTypes(CastOperator<TOpTableLookup>(op), ctx, props);
    }
    else if (MatchOperator<TOpIndexLookupJoin>(op)) {
        return ComputeTypes(CastOperator<TOpIndexLookupJoin>(op), ctx);
    }
    else if (MatchOperator<TOpGroupingSets>(op)) {
        return ComputeTypes(CastOperator<TOpGroupingSets>(op), ctx);
    }
    else if(MatchOperator<TOpAggregate>(op)) {
        return ComputeTypes(CastOperator<TOpAggregate>(op), ctx);
    }
    else if (MatchOperator<TOpWindow>(op)) {
        return ComputeTypes(CastOperator<TOpWindow>(op), ctx);
    }
    else if (MatchOperator<TOpCBOTree>(op)) {
        return ComputeTypes(CastOperator<TOpCBOTree>(op), ctx, props);
    } else if (MatchOperator<TOpTableEffect>(op)) {
        return ComputeTypes(CastOperator<TOpTableEffect>(op), ctx, props);
    }
    else {
        Y_ENSURE(false, "Invalid operator type in RBO type inference");
    }
}

} // anonymous namespace

TStatus TOpRoot::ComputeTypes(TRBOContext& ctx) {
    // Intrusive references control lifetime, not logical consumers. Count each
    // actual edge, including packed CBO trees and registered subplan roots.
    absl::flat_hash_map<const IOperator*, const TReplicate*> visited;
    TUnorderedIUs subplans;
    TVector<std::pair<IOperator*, const IOperator*>> pending{{this, nullptr}};
    while (!pending.empty()) {
        const auto [op, parent] = pending.back();
        pending.pop_back();
        Y_ENSURE(op, "Null RBO input");
        // Repeated child views are legal only when they refer to the same
        // canonical input slot in a shared replication binding.
        const auto* replicate = parent && parent->Kind == EOperator::Replicate
            ? &CastOperator<TOpReplicate>(*parent).GetReplicate() : nullptr;
        const auto [it, inserted] = visited.emplace(op, replicate);
        if (!inserted) {
            if (!replicate || it->second != replicate) {
                ctx.ExprCtx.AddError(TIssue(ctx.ExprCtx.GetPosition(op->Pos),
                    TStringBuilder() << "Shared RBO " << op->GetExplainName()
                        << ": sharing requires distinct ports of the same Replicate"));
                return TStatus::Error;
            }
            continue;
        }
        if (op->Kind == EOperator::CBOTree) {
            // Its virtual children are views of boundary slots, not extra edges.
            pending.emplace_back(CastOperator<TOpCBOTree>(op)->TreeRoot.Get(), op);
        } else {
            for (auto* child : op->GetChildren()) {
                pending.emplace_back(child, op);
            }
        }
        for (const auto call : op->GetSubplanIUs(PlanProps.Subplans)) {
            if (subplans.Add(call)) {
                pending.emplace_back(PlanProps.Subplans.At(call).Plan.Get(), nullptr);
            }
        }
    }

    for (const auto& item : *this) {
        auto status = ::NKikimr::NKqp::ComputeTypes(TIntrusivePtr<IOperator>(item.Current), ctx, PlanProps);
        if (status != TStatus::Ok) {
            return status;
        }
    }
    return TStatus::Ok;
}

} // namespace NKikimr::NKqp
