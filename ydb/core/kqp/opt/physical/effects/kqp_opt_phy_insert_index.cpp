#include <ydb/core/base/table_vector_index.h>

#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TExprBase MakeInsertIndexRows(const NYql::NNodes::TExprBase& inputRows, const TKikimrTableDescription& table,
    const THashSet<TStringBuf>& inputColumns, const TVector<TStringBuf>& indexColumns,
    TPositionHandle pos, TExprContext& ctx, bool useStage)
{
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));

    TVector<TExprBase> rowTuples;
    for (const auto& column : indexColumns) {
        auto columnAtom = ctx.NewAtom(pos, column);

        if (inputColumns.contains(column)) {
            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoMember>()
                    .Struct(inputRowArg)
                    .Name(columnAtom)
                    .Build()
                .Done();

            rowTuples.emplace_back(tuple);
        } else {
            auto columnType = table.GetColumnType(TString(column));

            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoNothing>()
                    .OptionalType(NCommon::BuildTypeExpr(pos, *columnType, ctx))
                    .Build()
                .Done();

            rowTuples.emplace_back(tuple);
        }
    }

    if (!useStage) {
        return Build<TCoMap>(ctx, pos)
            .Input(inputRows)
            .Lambda()
                .Args(inputRowArg)
                .Body<TCoAsStruct>()
                    .Add(rowTuples)
                    .Build()
                .Build()
            .Done();
    }

    auto computeRowsStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoIterator>()
                .List<TCoMap>()
                    .Input("rows")
                    .Lambda()
                        .Args(inputRowArg)
                        .Body<TCoAsStruct>()
                            .Add(rowTuples)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(computeRowsStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TKqpCnVectorResolve BuildVectorResolveOverPrecompute(NYql::NNodes::TDqPhyPrecompute& rowsPrecompute,
    NYql::NNodes::TExprBase originalInput, const TKqpTable& kqpTableNode,
    const TString& kqpIndexName, const TPositionHandle& pos, TExprContext& ctx)
{
    const TTypeAnnotationNode* originalAnnotation = originalInput.Ptr()->GetTypeAnn();
    YQL_ENSURE(originalAnnotation, "vector resolve received non-annotated input");

    TExprNode::TPtr input = originalInput.Ptr();
    const TTypeAnnotationNode* itemType = nullptr;

    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        itemType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
    } else {
        YQL_ENSURE(EnsureListType(*input, ctx), "stream or list is allowed as input of vector resolve");
        itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    }

    YQL_ENSURE(itemType->GetKind() == ETypeAnnotationKind::Struct);
    auto* columns = itemType->Cast<TStructExprType>();
    const TTypeAnnotationNode* resolveInputType = ctx.MakeType<TListExprType>(columns);

    return Build<TKqpCnVectorResolve>(ctx, pos)
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(rowsPrecompute)
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
        .Table(kqpTableNode)
        .InputType(ExpandType(pos, *resolveInputType, ctx))
        .Index(ctx.NewAtom(pos, kqpIndexName))
        .Done();
}

} // namespace

TExprBase KqpBuildInsertIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlInsertRowsIndex>()) {
        return node;
    }

    auto insert = node.Cast<TKqlInsertRowsIndex>();
    bool abortOnError = insert.OnConflict().Value() == "abort"sv;
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, insert.Table().Path());

    const bool isSink = NeedSinks(table, kqpCtx);

    auto indexes = BuildSecondaryIndexVector(table, insert.Pos(), ctx, nullptr);
    YQL_ENSURE(indexes);
    const bool canUseStreamIndex = kqpCtx.Config->EnableIndexStreamWrite
        && std::all_of(indexes.begin(), indexes.end(), [](const auto& index) {
            return index.second->Type == TIndexDescription::EType::GlobalSync;
        });

    const bool needPrecompute = !(isSink && abortOnError && canUseStreamIndex);

    if (!needPrecompute) {
        TVector<TStringBuf> insertColumns;
        THashSet<TStringBuf> inputColumnsSet;
        for (const auto& column : insert.Columns()) {
            YQL_ENSURE(inputColumnsSet.emplace(column.Value()).second);
            insertColumns.emplace_back(column.Value());
        }

        THashSet<TStringBuf> requiredIndexColumnsSet;
        for (const auto& [tableNode, indexDesc] : indexes) {
            for (const auto& column : indexDesc->KeyColumns) {
                if (requiredIndexColumnsSet.emplace(column).second && !inputColumnsSet.contains(column)) {
                    insertColumns.emplace_back(column);
                }
            }
        }

        if (insertColumns.size() == insert.Columns().Size()) {
            return Build<TKqlUpsertRows>(ctx, insert.Pos())
                .Table(insert.Table())
                .Input(insert.Input())
                .Columns(insert.Columns())
                .ReturningColumns(insert.ReturningColumns())
                .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
                .Settings(insert.Settings())
                .Done();
        } else {
            auto insertRows = MakeInsertIndexRows(
                insert.Input(), table, inputColumnsSet, insertColumns, insert.Pos(), ctx, false);
            return Build<TKqlUpsertRows>(ctx, insert.Pos())
                .Table(insert.Table())
                .Input(insertRows)
                .Columns(BuildColumnsList(insertColumns, insert.Pos(), ctx))
                .ReturningColumns(insert.ReturningColumns())
                .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
                .Settings(insert.Settings())
                .Done();
        }
    } else {
        THashSet<TStringBuf> inputColumnsSet;
        for (const auto& column : insert.Columns()) {
            inputColumnsSet.emplace(column.Value());
        }

        auto insertRows = MakeConditionalInsertRows(insert.Input(), table, inputColumnsSet, abortOnError, insert.Pos(), ctx);
        if (!insertRows) {
            return node;
        }

        auto insertRowsPrecompute = Build<TDqPhyPrecompute>(ctx, node.Pos())
            .Connection(insertRows.Cast())
            .Done();

        auto upsertTable = Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(insert.Table())
            .Input(insertRowsPrecompute)
            .Columns(insert.Columns())
            .ReturningColumns(insert.ReturningColumns())
            .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
            .Done();

        TVector<TExprBase> effects;
        effects.emplace_back(upsertTable);

        for (const auto& [tableNode, indexDesc] : indexes) {
            if (indexDesc->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
                TVector<TStringBuf> indexTableColumns;
                THashSet<TStringBuf> indexTableColumnSet;

                indexTableColumns.emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn);
                indexTableColumnSet.insert(NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn);

                for (const auto& column : table.Metadata->KeyColumnNames) {
                    if (indexTableColumnSet.insert(column).second) {
                        indexTableColumns.emplace_back(column);
                    }
                }

                for (const auto& column : indexDesc->DataColumns) {
                    YQL_ENSURE(inputColumnsSet.contains(column));
                    if (indexTableColumnSet.insert(column).second) {
                        indexTableColumns.emplace_back(column);
                    }
                }

                auto upsertIndexRows = BuildVectorResolveOverPrecompute(insertRowsPrecompute, insert.Input(), insert.Table(), indexDesc->Name, insert.Pos(), ctx);

                // Wrap into a stage + union, no idea why, but TKqlUpsertRows doesn't work without it

                auto upsertIndexStage = Build<TDqStage>(ctx, insert.Pos())
                    .Inputs()
                        .Add(upsertIndexRows)
                        .Build()
                    .Program()
                        .Args({"rows"})
                        .Body<TCoToStream>()
                            .Input("rows")
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Done();

                auto upsertUnion = Build<TDqCnUnionAll>(ctx, insert.Pos())
                    .Output()
                        .Stage(upsertIndexStage)
                        .Index().Build("0")
                        .Build()
                    .Done();

                auto upsertIndex = Build<TKqlUpsertRows>(ctx, insert.Pos())
                    .Table(tableNode)
                    .Input(upsertUnion)
                    .Columns(BuildColumnsList(indexTableColumns, insert.Pos(), ctx))
                    .ReturningColumns<TCoAtomList>().Build()
                    .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
                    .Done();

                effects.emplace_back(upsertIndex);

                //effects.emplace_back(KqpBuildVectorIndexInsert(indexDesc));
                continue;
            }

            THashSet<TStringBuf> indexTableColumnsSet;
            TVector<TStringBuf> indexTableColumns;

            for (const auto& column : indexDesc->KeyColumns) {
                YQL_ENSURE(indexTableColumnsSet.emplace(column).second);
                indexTableColumns.emplace_back(column);
            }

            for (const auto& column : table.Metadata->KeyColumnNames) {
                if (indexTableColumnsSet.insert(column).second) {
                    indexTableColumns.emplace_back(column);
                }
            }

            for (const auto& column : indexDesc->DataColumns) {
                if (inputColumnsSet.contains(column)) {
                    YQL_ENSURE(indexTableColumnsSet.emplace(column).second);
                    indexTableColumns.emplace_back(column);
                }
            }

            auto upsertIndexRows = MakeInsertIndexRows(insertRowsPrecompute, table, inputColumnsSet, indexTableColumns,
                insert.Pos(), ctx, true);

            auto upsertIndex = Build<TKqlUpsertRows>(ctx, insert.Pos())
                .Table(tableNode)
                .Input(upsertIndexRows)
                .Columns(BuildColumnsList(indexTableColumns, insert.Pos(), ctx))
                .ReturningColumns<TCoAtomList>().Build()
                .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
                .Done();

            effects.emplace_back(upsertIndex);
        }

        return Build<TExprList>(ctx, insert.Pos())
            .Add(effects)
            .Done();
    }
}

} // namespace NKikimr::NKqp::NOpt
