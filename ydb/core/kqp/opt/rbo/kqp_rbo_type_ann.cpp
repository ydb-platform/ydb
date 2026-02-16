#include "kqp_operator.h"
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

using TStatus = NYql::IGraphTransformer::TStatus;

namespace {
using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

THashSet<TString> SupportedAggregationFunctions = {"sum", "min", "max", "count", "distinct", "avg"};

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

TStatus ComputeTypes(std::shared_ptr<TOpRead> read, TRBOContext & ctx) {
    auto table = ResolveTable(read->TableCallable.Get(), ctx.ExprCtx, ctx.KqpCtx.Cluster, *ctx.KqpCtx.Tables);
    if (!table.second) {
        YQL_CLOG(TRACE, CoreDq) << "Type annotation for Read, did not resolve table";
        return TStatus::Error;
    }

    YQL_ENSURE(table.second->Metadata, "Expected loaded metadata");

    auto meta = table.second->Metadata;

    TVector<TCoAtom> columns;
    for (auto c : read->Columns) {
        columns.push_back(Build<TCoAtom>(ctx.ExprCtx, read->Pos).Value(c).Done());
    }

    auto columnsList = Build<TCoAtomList>(ctx.ExprCtx, read->Pos).Add(columns).Done();

    const TTypeAnnotationNode* rowType = GetReadTableRowType(ctx.ExprCtx, *ctx.KqpCtx.Tables, ctx.KqpCtx.Cluster, 
        table.first, columnsList, ctx.KqpCtx.Config->SystemColumnsEnabled());
    if (!rowType) {
        YQL_CLOG(TRACE, CoreDq) << "Type annotation for Read, did not get row type";
        return TStatus::Error;
    }

    TVector<const TItemExprType*> structItemTypes = rowType->Cast<TStructExprType>()->GetItems();
    TVector<const TItemExprType*> newItemTypes;
    for (const auto* t : structItemTypes) {
        TString columnName = TString(t->GetName());
        auto it = std::find(read->Columns.begin(), read->Columns.end(), columnName);
        auto columnIndex = std::distance(read->Columns.begin(), it);
        auto fullName = read->OutputIUs[columnIndex].GetFullName();
        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(fullName, t->GetItemType()));
    }

    auto newStructType = ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes);
    read->Type = ctx.ExprCtx.MakeType<TListExprType>(newStructType);

    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpEmptySource> emptySource, TRBOContext & ctx) {
    TVector<const TItemExprType*> resultItems;
    auto resultType = ctx.ExprCtx.MakeType<TStructExprType>(resultItems);

    emptySource->Type = ctx.ExprCtx.MakeType<TListExprType>(resultType);

    return TStatus::Ok;
}

const TStructExprType* AddSubplanTypes(const TStructExprType* itemType, TVector<TInfoUnit> subplanContextIUs, TRBOContext& ctx, TPlanProps& props) {
    TVector<const TItemExprType*> structItemTypes;
    for (const auto *item : itemType->GetItems()) {
        structItemTypes.push_back(item);
    }

    for (const auto& iu : subplanContextIUs) {
        const TTypeAnnotationNode* subplanType;
        auto subplanEntry = props.Subplans.PlanMap.at(iu);
        if (subplanEntry.Type == ESubplanType::EXPR) {
            auto subplan = subplanEntry.Plan;
            auto subplanTupleType = subplan->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            subplanType = subplanTupleType->GetItems()[0]->GetItemType();
        } else {
            if (!props.PgSyntax) {
                subplanType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Bool);
            } else {
                subplanType = ctx.ExprCtx.MakeType<TPgExprType>(NYql::NPg::LookupType("bool").TypeId);
            }
        }
        auto newType = ctx.ExprCtx.MakeType<TItemExprType>(iu.GetFullName(), subplanType);
        structItemTypes.push_back(newType);
    }

    return ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes);
}

TStatus ComputeTypes(std::shared_ptr<TOpFilter> filter, TRBOContext& ctx, TPlanProps& props) {
    const TTypeAnnotationNode* inputType = filter->GetInput()->Type;
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, inputType: " << *inputType;

    auto itemType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType: " << *(TTypeAnnotationNode*)itemType;

    auto filterIUs = filter->GetFilterIUs(props);
    TVector<TInfoUnit> subplanContextIUs;
    for (const auto& iu : filterIUs) {
        if (iu.IsSubplanContext()) {
            subplanContextIUs.push_back(iu);
        }
    }
    if (!subplanContextIUs.empty()) {
        itemType = AddSubplanTypes(itemType, subplanContextIUs, ctx, props);
    }
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType after scalars: " << *(TTypeAnnotationNode*)itemType;


    auto& lambda = filter->FilterExpr.Node;

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.ExprCtx)) {
        YQL_CLOG(TRACE, CoreDq) << "Could not update lambda arg types";
        return IGraphTransformer::TStatus::Error;
    }

    ctx.TypeAnnTransformer.Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);

    } while (status == IGraphTransformer::TStatus::Repeat);

    auto lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        YQL_CLOG(TRACE, CoreDq) << "Could not infer lambda types, status = " << status;
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

    filter->Type = inputType;

    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpMap> map, TRBOContext& ctx) {
    TVector<const TItemExprType*> resStructItemTypes;
    const TTypeAnnotationNode* inputType = map->GetInput()->Type;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto typeItems = structType->GetItems();

    if (!map->Project) {
        const TTypeAnnotationNode* inputType = map->GetInput()->Type;
        auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        for (const auto* item : structType->GetItems()) {
            resStructItemTypes.push_back(item);
        }
    }

    for (auto& mapElement : map->MapElements) {
        // This is type annotation update inplace, which is different comparing to yql type annotation.
        auto& lambda = mapElement.GetExpressionRef().Node;
        if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
            return IGraphTransformer::TStatus::Error;
        }

        ctx.TypeAnnTransformer.Rewind();
        IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
        do {
            status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);
        // Could we have an infinity loop?
        } while (status == IGraphTransformer::TStatus::Repeat);

        if (status == IGraphTransformer::TStatus::Error) {
            return status;
        }

        auto lambdaType = lambda->GetTypeAnn();
        Y_ENSURE(lambdaType);
        auto mapLambdaType = ctx.ExprCtx.MakeType<TItemExprType>(mapElement.GetElementName().GetFullName(), lambdaType);
        resStructItemTypes.push_back(mapLambdaType);
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resStructItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);
    map->Type = resultAnn;
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Map done: " << *resultAnn;

    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpAddDependencies> addDeps, TRBOContext& ctx) {
    const TTypeAnnotationNode* inputType = addDeps->GetInput()->Type;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto resStructItemTypes = structType->GetItems();

    for (size_t i=0; i<addDeps->Dependencies.size(); i++) {
        resStructItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(addDeps->Dependencies[i].GetFullName(), addDeps->Types[i]));
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resStructItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);
    addDeps->Type = resultAnn;
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpUnionAll> unionAll, TRBOContext & ctx) {
    Y_UNUSED(ctx);
    auto leftInputType = unionAll->GetLeftInput()->Type;
    // TODO: Add sanity checks.
    unionAll->Type = leftInputType;
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpAggregate> aggregate, TRBOContext& ctx) {
    auto inputType = aggregate->GetInput()->Type;
    const auto* structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TVector<const TItemExprType*> newItemTypes;
    THashMap<TString, const TTypeAnnotationNode*> aggTraitsMap;
    for (const auto* itemType : structType->GetItems()) {
        const auto itemName = itemType->GetName();
        aggTraitsMap.emplace(itemName, itemType->GetItemType());
    }

    for (const auto& keyColumn : aggregate->KeyColumns) {
        auto it = aggTraitsMap.find(keyColumn.GetFullName());
        Y_ENSURE(it != aggTraitsMap.end());
        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(it->first, it->second));
    }

    for (const auto& traits : aggregate->AggregationTraitsList) {
        const auto originalColName = traits.OriginalColName.GetFullName();
        const auto& aggFunction = traits.AggFunction;
        const auto resultColName = traits.ResultColName.GetFullName();
        auto it = aggTraitsMap.find(originalColName);
        Y_ENSURE(it != aggTraitsMap.end());
        const auto* aggFieldType = it->second;
        TPositionHandle dummyPos;

        if (aggFunction == "count") {
            aggFieldType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
        } else if (aggFunction == "sum") {
            Y_ENSURE(GetSumResultType(dummyPos, *it->second, aggFieldType, ctx.ExprCtx),
                        "Unsupported type for sum aggregation function");
        } else if (aggFunction == "avg") {
            Y_ENSURE(GetAvgResultType(dummyPos, *it->second, aggFieldType, ctx.ExprCtx),
                        "Unsupported type for avg aggregation function");
        }

        newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(resultColName, aggFieldType));
    }

    aggregate->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpJoin> join, TRBOContext& ctx) {
    auto leftInputType = join->GetLeftInput()->Type;
    auto rightInputType = join->GetRightInput()->Type;

    auto leftItemType = leftInputType->Cast<TListExprType>()->GetItemType();
    auto rightItemType = rightInputType->Cast<TListExprType>()->GetItemType();

    TVector<const TItemExprType*> structItemTypes;
    TVector<const TItemExprType*> leftItemTypes = leftItemType->Cast<TStructExprType>()->GetItems();
    TVector<const TItemExprType*> rightItemTypes = rightItemType->Cast<TStructExprType>()->GetItems();

    if (join->JoinKind == "LeftOnly" || join->JoinKind == "LeftSemi") {
        rightItemTypes = {};
    }
    if (join->JoinKind == "RightOnly" || join->JoinKind == "RightSemi") {
        leftItemTypes = {};
    }

    structItemTypes.insert(structItemTypes.end(), leftItemTypes.begin(), leftItemTypes.end());
    structItemTypes.insert(structItemTypes.end(), rightItemTypes.begin(), rightItemTypes.end());

    auto resultStructType = ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultStructType);
    join->Type = resultAnn;

    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpLimit> limit, TRBOContext & ctx) {
    Y_UNUSED(ctx);
    auto inputType = limit->GetInput()->Type;
    // TODO: Add sanity checks.
    limit->Type = inputType;
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpSort> sort, TRBOContext & ctx) {
    Y_UNUSED(ctx);
    auto inputType = sort->GetInput()->Type;
    // TODO: Add sanity checks.
    sort->Type = inputType;
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<IOperator> op, TRBOContext & ctx, TPlanProps& props);

TStatus ComputeTypes(std::shared_ptr<TOpCBOTree> cboTree, TRBOContext &ctx, TPlanProps& props) {
    for (auto op : cboTree->TreeNodes) {
        if (auto status = ComputeTypes(op, ctx, props); status != TStatus::Ok) {
            return status;
        }
    }
    cboTree->Type = cboTree->TreeRoot->Type;
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<IOperator> op, TRBOContext & ctx, TPlanProps& props) {
    if (MatchOperator<TOpEmptySource>(op)) {
        return ComputeTypes(CastOperator<TOpEmptySource>(op), ctx);
    }
    else if (MatchOperator<TOpRead>(op)) {
        return ComputeTypes(CastOperator<TOpRead>(op), ctx);
    }
    else if(MatchOperator<TOpFilter>(op)) {
        return ComputeTypes(CastOperator<TOpFilter>(op), ctx, props);
    }
    else if(MatchOperator<TOpMap>(op)) {
        return ComputeTypes(CastOperator<TOpMap>(op), ctx);
    }
    else if(MatchOperator<TOpAddDependencies>(op)) {
        return ComputeTypes(CastOperator<TOpAddDependencies>(op), ctx);
    }
    else if(MatchOperator<TOpJoin>(op)) {
        return ComputeTypes(CastOperator<TOpJoin>(op), ctx);
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
    else if(MatchOperator<TOpAggregate>(op)) {
        return ComputeTypes(CastOperator<TOpAggregate>(op), ctx);
    }
    else if (MatchOperator<TOpCBOTree>(op)) {
        return ComputeTypes(CastOperator<TOpCBOTree>(op), ctx, props);
    }
    else {
        Y_ENSURE(false, "Invalid operator type in RBO type inference");
    }
}

}

namespace NKikimr {
namespace NKqp {

TStatus TOpRoot::ComputeTypes(TRBOContext& ctx) {
    for (auto it = begin(); it != end(); it++) {
        auto status = ::ComputeTypes((*it).Current, ctx, PlanProps);
        if (status != TStatus::Ok) {
            return status;
        }
    }
    return TStatus::Ok;
}

}
}