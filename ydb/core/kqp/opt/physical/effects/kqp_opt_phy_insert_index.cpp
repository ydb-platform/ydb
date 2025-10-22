#include <ydb/core/base/table_index.h>
#include <ydb/core/base/fulltext.h>

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
                if (inputColumnsSet.contains(column) && indexTableColumnsSet.emplace(column).second) {
                    indexTableColumns.emplace_back(column);
                }
            }

            std::optional<TExprBase> upsertIndexRows;
            switch (indexDesc->Type) {
                case TIndexDescription::EType::GlobalSync:
                case TIndexDescription::EType::GlobalAsync:
                case TIndexDescription::EType::GlobalSyncUnique: {
                    upsertIndexRows = MakeInsertIndexRows(insertRowsPrecompute, table, inputColumnsSet, indexTableColumns,
                        insert.Pos(), ctx, true);
                    break;
                }
                case TIndexDescription::EType::GlobalSyncVectorKMeansTree: {
                    upsertIndexRows = MakeInsertIndexRows(insertRowsPrecompute, table, inputColumnsSet, indexTableColumns,
                        insert.Pos(), ctx, true);
                    if (indexDesc->KeyColumns.size() > 1) {
                        // First resolve prefix IDs using StreamLookup
                        const auto& prefixTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << insert.Table().Path().Value()
                            << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NKMeans::PrefixTable);
                        if (prefixTable.Metadata->Columns.at(NTableIndex::NKMeans::IdColumn).DefaultKind == NKikimrKqp::TKqpColumnMetadataProto::DEFAULT_KIND_SEQUENCE) {
                            auto res = BuildVectorIndexPrefixRowsWithNew(table, prefixTable, indexDesc, upsertIndexRows.value(), indexTableColumns, insert.Pos(), ctx);
                            upsertIndexRows = std::move(res.first);
                            effects.emplace_back(std::move(res.second));
                        } else {
                            // Handle old prefixed vector index tables without the sequence
                            upsertIndexRows = BuildVectorIndexPrefixRows(table, prefixTable, true, indexDesc, upsertIndexRows.value(), indexTableColumns, insert.Pos(), ctx);
                        }
                    }
                    upsertIndexRows = BuildVectorIndexPostingRows(table, insert.Table(), indexDesc->Name, indexTableColumns,
                        upsertIndexRows.value(), true, insert.Pos(), ctx);
                    indexTableColumns = BuildVectorIndexPostingColumns(table, indexDesc);
                    break;
                }
                case TIndexDescription::EType::GlobalFulltext: {
                    // For fulltext indexes, we need to tokenize the text and create index rows and refill index columns
                    upsertIndexRows = BuildFulltextIndexRows(table, indexDesc, insertRowsPrecompute, inputColumnsSet, indexTableColumns, /*includeDataColumns=*/true,
                        insert.Pos(), ctx);
                    break;
                }
            }
            Y_ENSURE(upsertIndexRows.has_value());
            Y_ENSURE(indexTableColumns);

            auto upsertIndex = Build<TKqlUpsertRows>(ctx, insert.Pos())
                .Table(tableNode)
                .Input(upsertIndexRows.value())
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
