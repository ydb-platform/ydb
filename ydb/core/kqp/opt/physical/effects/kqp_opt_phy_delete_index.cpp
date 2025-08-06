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

} // namespace

TExprBase KqpBuildDeleteIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlDeleteRowsIndex>()) {
        return node;
    }

    auto del = node.Cast<TKqlDeleteRowsIndex>();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, del.Table().Path());
    const auto& pk = table.Metadata->KeyColumnNames;

    auto payloadSelector = Build<TCoLambda>(ctx, del.Pos())
        .Args({"stub"})
        .Body<TCoVoid>().Build()
        .Done();

    auto condenseResult = CondenseInputToDictByPk(del.Input(), table, payloadSelector, ctx);
    if (!condenseResult) {
        return node;
    }

    auto lookupKeys = PrecomputeDictKeys(*condenseResult, del.Pos(), ctx);

    const auto indexes = BuildSecondaryIndexVector(table, del.Pos(), ctx, nullptr, true);
    YQL_ENSURE(indexes);
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

        auto deleteIndexKeys = MakeRowsFromDict(lookupDict.Cast(), pk, indexTableColumns, del.Pos(), ctx);

        if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
            // Generate input type for vector resolve
            TVector<const TItemExprType*> rowItems;
            for (const auto& column : indexTableColumns) {
                auto type = table.GetColumnType(TString(column));
                YQL_ENSURE(type, "No key column: " << column);
                auto itemType = ctx.MakeType<TItemExprType>(column, type);
                YQL_ENSURE(itemType->Validate(del.Pos(), ctx));
                rowItems.push_back(itemType);
            }
            auto rowType = ctx.MakeType<TStructExprType>(rowItems);
            YQL_ENSURE(rowType->Validate(del.Pos(), ctx));
            const TTypeAnnotationNode* resolveInputType = ctx.MakeType<TListExprType>(rowType);

            auto resolveOutput = Build<TKqpCnVectorResolve>(ctx, del.Pos())
                .Output()
                    .Stage<TDqStage>()
                        .Inputs()
                            .Add(deleteIndexKeys)
                            .Build()
                        .Program()
                            .Args({"vector_resolve_rows"})
                            .Body<TCoToStream>()
                                .Input("vector_resolve_rows")
                                .Build()
                            .Build()
                        .Settings().Build()
                        .Build()
                    .Index().Build(0)
                    .Build()
                .Table(del.Table())
                .InputType(ExpandType(del.Pos(), *resolveInputType, ctx))
                .Index(ctx.NewAtom(del.Pos(), indexDesc->Name))
                .Done();

            auto deleteIndexStage = Build<TDqStage>(ctx, del.Pos())
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

            auto deleteUnion = Build<TDqCnUnionAll>(ctx, del.Pos())
                .Output()
                    .Stage(deleteIndexStage)
                    .Index().Build("0")
                    .Build()
                .Done();

            auto indexDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
                .Table(tableNode)
                .Input(deleteUnion)
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

} // namespace NKikimr::NKqp::NOpt
