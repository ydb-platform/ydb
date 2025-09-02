#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TDqPhyPrecompute PrecomputeDictKeys(const TCondenseInputResult& condenseResult, TPositionHandle pos,
    TExprContext& ctx)
{
    auto computeDictKeysStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body<TCoMap>()
                .Input(condenseResult.Stream)
                .Lambda()
                    .Args({"dict"})
                    .Body<TCoDictKeys>()
                        .Dict("dict")
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeDictKeysStage)
               .Index().Build("0")
               .Build()
           .Build()
        .Done();
}

TDqStageBase ReadTableToStage(const TExprBase& expr, TExprContext& ctx) {
    if (expr.Maybe<TDqStageBase>()) {
        return expr.Cast<TDqStageBase>();
    }
    if (expr.Maybe<TDqCnUnionAll>()) {
        return expr.Cast<TDqCnUnionAll>().Output().Stage();
    }
    auto pos = expr.Pos();
    TVector<TExprNode::TPtr> inputs;
    TVector<TExprNode::TPtr> args;
    TNodeOnNodeOwnedMap replaces;
    int i = 1;
    VisitExpr(expr.Ptr(), [&](const TExprNode::TPtr& node) {
        TExprBase expr(node);
        if (auto cast = expr.Maybe<TDqCnUnionAll>()) {
            auto newArg = ctx.NewArgument(pos, TStringBuilder() << "rows" << i);
            inputs.emplace_back(node);
            args.emplace_back(newArg);
            replaces.emplace(expr.Raw(), newArg);
            return false;
        }
        return true;
    });
    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
            .Build()
        .Program()
            .Args(args)
            .Body(ctx.ReplaceNodes(expr.Ptr(), replaces))
            .Build()
        .Settings()
            .Build()
        .Done();
}

TExprBase BuildDeleteIndexStagesImpl(const TKikimrTableDescription& table,
    const TSecondaryIndexes& indexes, const TKqlDeleteRowsIndex& del,
    const TExprBase& lookupKeys, std::function<TExprBase(const TVector<TStringBuf>&)> project,
    TExprContext& ctx) {
    const auto& pk = table.Metadata->KeyColumnNames;

    auto tableDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
        .Table(del.Table())
        .Input(lookupKeys)
        .ReturningColumns(del.ReturningColumns())
        .IsBatch(ctx.NewAtom(del.Pos(), "false"))
        .Done();

    TVector<TExprBase> effects;
    effects.emplace_back(tableDelete);

    for (const auto& [tableNode, indexDesc] : indexes) {
        THashSet<TStringBuf> indexTableColumnsSet;
        TVector<TStringBuf> indexTableColumns;

        for (const auto& column : indexDesc->KeyColumns) {
            YQL_ENSURE(indexTableColumnsSet.emplace(column).second);
            indexTableColumns.emplace_back(column);
        }

        for (const auto& column : pk) {
            if (indexTableColumnsSet.insert(column).second) {
                indexTableColumns.emplace_back(column);
            }
        }

        auto deleteIndexKeys = project(indexTableColumns);

        if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
            auto resolveUnion = BuildVectorIndexPostingRows(table, del.Table(), indexDesc->Name,
                indexTableColumns, deleteIndexKeys, false, del.Pos(), ctx);

            auto indexDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
                .Table(tableNode)
                .Input(resolveUnion)
                .ReturningColumns<TCoAtomList>().Build()
                .IsBatch(ctx.NewAtom(del.Pos(), "false"))
                .Done();

            effects.emplace_back(indexDelete);
        } else {
            auto indexDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
                .Table(tableNode)
                .Input(deleteIndexKeys)
                .ReturningColumns<TCoAtomList>().Build()
                .IsBatch(ctx.NewAtom(del.Pos(), "false"))
                .Done();

            effects.emplace_back(std::move(indexDelete));
        }
    }

    return Build<TExprList>(ctx, del.Pos())
        .Add(effects)
        .Done();
}

} // namespace

TExprBase BuildVectorIndexPostingRows(const TKikimrTableDescription& table,
    const TKqpTable& tableNode,
    const TString& indexName,
    const TVector<TStringBuf>& indexTableColumns,
    const TExprBase& inputRows,
    bool withData,
    TPositionHandle pos, TExprContext& ctx) {
    // Generate input type for vector resolve
    TVector<const TItemExprType*> rowItems;
    for (const auto& column : indexTableColumns) {
        auto type = table.GetColumnType(TString(column));
        YQL_ENSURE(type, "No key column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        YQL_ENSURE(itemType->Validate(pos, ctx));
        rowItems.push_back(itemType);
    }
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);
    YQL_ENSURE(rowType->Validate(pos, ctx));
    const TTypeAnnotationNode* resolveInputType = ctx.MakeType<TListExprType>(rowType);

    auto resolveInput = (inputRows.Maybe<TDqCnUnionAll>()
        ? inputRows.Maybe<TDqCnUnionAll>().Cast().Output()
        : Build<TDqOutput>(ctx, pos)
            .Stage(ReadTableToStage(inputRows, ctx))
            .Index().Build(0)
            .Done());

    auto resolveOutput = Build<TKqpCnVectorResolve>(ctx, pos)
        .Output(resolveInput)
        .Table(tableNode)
        .InputType(ExpandType(pos, *resolveInputType, ctx))
        .Index(ctx.NewAtom(pos, indexName))
        .WithData(ctx.NewAtom(pos, withData ? "true" : "false"))
        .Done();

    auto resolveStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(resolveOutput)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input("rows")
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(resolveStage)
            .Index().Build(0)
            .Build()
        .Done();
}

TExprBase KqpBuildDeleteIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlDeleteRowsIndex>()) {
        return node;
    }

    auto del = node.Cast<TKqlDeleteRowsIndex>();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, del.Table().Path());
    const auto& pk = table.Metadata->KeyColumnNames;

    const auto indexes = BuildSecondaryIndexVector(table, del.Pos(), ctx);
    YQL_ENSURE(indexes);

    // Skip lookup means that the input already has all required columns and we only need to project them
    auto settings = TKqpDeleteRowsIndexSettings::Parse(del);

    if (settings.SkipLookup) {
        auto lookupKeys = ProjectColumns(del.Input(), pk, ctx);
        return BuildDeleteIndexStagesImpl(table, indexes, del, lookupKeys, [&](const TVector<TStringBuf>& indexTableColumns) {
            return ProjectColumns(del.Input(), indexTableColumns, ctx);
        }, ctx);
    }

    auto payloadSelector = Build<TCoLambda>(ctx, del.Pos())
        .Args({"stub"})
        .Body<TCoVoid>().Build()
        .Done();

    auto condenseResult = CondenseInputToDictByPk(del.Input(), table, payloadSelector, ctx);
    if (!condenseResult) {
        return node;
    }

    auto lookupKeys = PrecomputeDictKeys(*condenseResult, del.Pos(), ctx);

    THashSet<TString> keyColumns;
    for (const auto& pair : indexes) {
        for (const auto& col : pair.second->KeyColumns) {
            keyColumns.emplace(col);
        }
    }

    auto lookupDict = PrecomputeTableLookupDict(lookupKeys, table, {}, keyColumns, del.Pos(), ctx);
    if (!lookupDict) {
        return node;
    }

    return BuildDeleteIndexStagesImpl(table, indexes, del, lookupKeys, [&](const TVector<TStringBuf>& indexTableColumns) {
        return MakeRowsFromDict(lookupDict.Cast(), pk, indexTableColumns, del.Pos(), ctx);
    }, ctx);
}

} // namespace NKikimr::NKqp::NOpt
