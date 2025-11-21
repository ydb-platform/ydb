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

THashSet<TString> SupportedAggregationFunctions = {"sum", "min", "max", "count", "distinct"};

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
    for (auto t : structItemTypes) {
        TString columnName = TString(t->GetName());
        TString fullName = read->Alias != "" ? ( "_alias_" + read->Alias + "." + columnName ) : columnName;
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

const TStructExprType* AddScalarTypes(const TStructExprType* itemType, TVector<TInfoUnit> scalarContextIUs, TRBOContext & ctx, TPlanProps& props) {
    TVector<const TItemExprType*> structItemTypes;
    for (auto t : itemType->GetItems()) {
        structItemTypes.push_back(t);
    }

    for (auto iu : scalarContextIUs) {
        auto subplan = props.ScalarSubplans.PlanMap.at(iu);
        auto subplanType = subplan->Type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        auto scalarExprType = subplanType->GetItems()[0];

        auto newType = ctx.ExprCtx.MakeType<TItemExprType>(iu.GetFullName(), scalarExprType->GetItemType());
        structItemTypes.push_back(newType);
    }

    return ctx.ExprCtx.MakeType<TStructExprType>(structItemTypes);
}

TStatus ComputeTypes(std::shared_ptr<TOpFilter> filter, TRBOContext & ctx, TPlanProps& props) {
    const TTypeAnnotationNode* inputType = filter->GetInput()->Type;
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, inputType: " << *inputType;

    auto itemType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType: " << *(TTypeAnnotationNode*)itemType;

    auto filterIUs = filter->GetFilterIUs(props);
    TVector<TInfoUnit> scalarContextIUs;
    for (auto iu : filterIUs ) {
        if (iu.ScalarContext) {
            scalarContextIUs.push_back(iu);
        }
    }
    if (!scalarContextIUs.empty()) {
        itemType = AddScalarTypes(itemType, scalarContextIUs, ctx, props);
    }
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Filter, itemType after scalars: " << *(TTypeAnnotationNode*)itemType;


    auto& lambda = filter->FilterLambda;

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.ExprCtx)) {
        YQL_CLOG(TRACE, CoreDq) << "Could not update lambda arg types";
        return IGraphTransformer::TStatus::Error;
    }

    ctx.TypeAnnTransformer->Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer->Transform(lambda, lambda, ctx.ExprCtx);

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

TStatus ComputeTypes(std::shared_ptr<TOpMap> map, TRBOContext & ctx) {
    TVector<const TItemExprType*> resStructItemTypes;

    const TTypeAnnotationNode* inputType = map->GetInput()->Type;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto typeItems = structType->GetItems();

    if (!map->Project) {
        const TTypeAnnotationNode* inputType = map->GetInput()->Type;
        auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        for (auto t : structType->GetItems()) {
            resStructItemTypes.push_back(t);
        }
    }

    for (auto & mapEl : map->MapElements) {
        if (std::holds_alternative<TInfoUnit>(mapEl.second)) {
            TInfoUnit from = std::get<TInfoUnit>(mapEl.second);
            auto typeIt = std::find_if(typeItems.begin(), typeItems.end(), [&from](const TItemExprType* t){
                return from.GetFullName() == t->GetName();
            });
            Y_ENSURE(typeIt!=typeItems.end());

            auto renameType = ctx.ExprCtx.MakeType<TItemExprType>(mapEl.first.GetFullName(), (*typeIt)->GetItemType());
            resStructItemTypes.push_back(renameType);
        }
        else {
            auto & lambda = std::get<TExprNode::TPtr>(mapEl.second);
            if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
                return IGraphTransformer::TStatus::Error;
            }

            ctx.TypeAnnTransformer->Rewind();
            IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
            do {
                status = ctx.TypeAnnTransformer->Transform(lambda, lambda, ctx.ExprCtx);

            } while (status == IGraphTransformer::TStatus::Repeat);

            auto lambdaType = lambda->GetTypeAnn();
            Y_ENSURE(lambdaType);

            auto mapLambdaType = ctx.ExprCtx.MakeType<TItemExprType>(mapEl.first.GetFullName(), lambdaType);
            resStructItemTypes.push_back(mapLambdaType);
        }
    }

    auto resultItemType = ctx.ExprCtx.MakeType<TStructExprType>(resStructItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.ExprCtx.MakeType<TListExprType>(resultItemType);

    map->Type = resultAnn;

    YQL_CLOG(TRACE, CoreDq) << "Type annotation for Map done: " << *resultAnn;

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
    THashMap<TString, std::pair<TString, TString>> aggTraitsMap;
    for (const auto& aggTraits : aggregate->AggregationTraitsList) {
        const auto originalColName = TString(aggTraits.OriginalColName.GetFullName());
        const auto resultColName = TString(aggTraits.ResultColName.GetFullName());
        const auto funcName = TString(aggTraits.AggFunction);
        aggTraitsMap[originalColName] = {resultColName, funcName};
    }

    THashSet<TString> keyColumns;
    for (const auto& key : aggregate->KeyColumns) {
        keyColumns.insert(key.GetFullName());
    }

    TVector<const TItemExprType*> newItemTypes;
    for (const auto* itemType : structType->GetItems()) {
        // The type of the column could be changed after aggregation.
        const auto itemName = itemType->GetName();
        if (auto it = aggTraitsMap.find(itemName); it != aggTraitsMap.end()) {
            const auto& resultColName = it->second.first;
            const auto& aggFunction = it->second.second;
            Y_ENSURE(SupportedAggregationFunctions.count(aggFunction), "Unsupported aggregation function " + aggFunction);

            const TTypeAnnotationNode* aggFieldType = itemType->GetItemType();
            if (aggFunction == "count") {
                aggFieldType = ctx.ExprCtx.MakeType<TDataExprType>(EDataSlot::Uint64);
            } else if (aggFunction == "sum") {
                TPositionHandle dummyPos;
                Y_ENSURE(GetSumResultType(dummyPos, *itemType->GetItemType(), aggFieldType, ctx.ExprCtx),
                         "Unsupported type for sum aggregation function");
            }
            newItemTypes.push_back(ctx.ExprCtx.MakeType<TItemExprType>(resultColName, aggFieldType));
        } else if (keyColumns.contains(itemName)) {
            newItemTypes.push_back(itemType);
        }
    }

    aggregate->Type = ctx.ExprCtx.MakeType<TListExprType>(ctx.ExprCtx.MakeType<TStructExprType>(newItemTypes));
    return TStatus::Ok;
}

TStatus ComputeTypes(std::shared_ptr<TOpJoin> join, TRBOContext& ctx) {
    // FIXME: This works correctly only for inner joins, other join types 
    auto leftInputType = join->GetLeftInput()->Type;
    auto rightInputType = join->GetRightInput()->Type;

    auto leftItemType = leftInputType->Cast<TListExprType>()->GetItemType();
    auto rightItemType = rightInputType->Cast<TListExprType>()->GetItemType();

    TVector<const TItemExprType*> structItemTypes = leftItemType->Cast<TStructExprType>()->GetItems();

    for (auto item : rightItemType->Cast<TStructExprType>()->GetItems()){
        structItemTypes.push_back(item);
    }

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
    else if(MatchOperator<TOpJoin>(op)) {
        return ComputeTypes(CastOperator<TOpJoin>(op), ctx);
    }
    else if(MatchOperator<TOpUnionAll>(op)) {
        return ComputeTypes(CastOperator<TOpUnionAll>(op), ctx);
    }
    else if(MatchOperator<TOpLimit>(op)) {
        return ComputeTypes(CastOperator<TOpLimit>(op), ctx);
    }
    else if(MatchOperator<TOpAggregate>(op)) {
        return ComputeTypes(CastOperator<TOpAggregate>(op), ctx);
    }
    else {
        Y_ENSURE(false, "Invalid operator type in RBO type inference");
    }
}

}

namespace NKikimr {
namespace NKqp {

TStatus TOpRoot::ComputeTypes(TRBOContext & ctx) {
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