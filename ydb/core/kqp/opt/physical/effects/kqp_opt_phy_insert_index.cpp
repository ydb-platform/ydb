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
            const auto* optionalColumnType = columnType->IsOptionalOrNull()
                ? columnType
                : ctx.MakeType<TOptionalExprType>(columnType);

            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoNothing>()
                    .OptionalType(NCommon::BuildTypeExpr(pos, *optionalColumnType, ctx))
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

    auto indexes = BuildAffectedIndexTables(table, insert.Pos(), ctx, nullptr);
    YQL_ENSURE(indexes);
    const bool useStreamIndex = isSink && kqpCtx.Config->GetEnableIndexStreamWrite();

    const bool needPrecompute = !useStreamIndex
        || !abortOnError
        || std::any_of(indexes.begin(), indexes.end(), [](const auto& index) {
            return index.second->Type == TIndexDescription::EType::GlobalSyncVectorKMeansTree
                || index.second->Type == TIndexDescription::EType::GlobalFulltextPlain
                || index.second->Type == TIndexDescription::EType::GlobalFulltextRelevance;
        });

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

    std::optional<TExprBase> insertRows;
    if (needPrecompute) {
        // TODO: don't use precompute here!
        auto conditionalInsertRows = MakeConditionalInsertRows(insert.Input(), table, inputColumnsSet, abortOnError, insert.Pos(), ctx);
        if (!conditionalInsertRows) {
            return node;
        }

        insertRows = Build<TDqPhyPrecompute>(ctx, node.Pos())
            .Connection(conditionalInsertRows.Cast())
            .Done();
    } else {
        insertRows = (insertColumns.size() == insert.Columns().Size())
            ? insert.Input()
            : MakeInsertIndexRows(
                insert.Input(), table, inputColumnsSet, insertColumns, insert.Pos(), ctx, false);
    }
    AFL_ENSURE(insertRows);

    TVector<TExprBase> effects;

    if (useStreamIndex) {
        effects.emplace_back(Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(insert.Table())
            .Input(*insertRows)
            .Columns(BuildColumnsList(insertColumns, insert.Pos(), ctx))
            .ReturningColumns(insert.ReturningColumns())
            .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
            .DefaultColumns<TCoAtomList>().Build()
            .Settings(insert.Settings())
            .Done());
    } else {
        effects.emplace_back(Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(insert.Table())
            .Input(*insertRows)
            .Columns(insert.Columns())
            .ReturningColumns(insert.ReturningColumns())
            .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
            .DefaultColumns<TCoAtomList>().Build()
            .Done());
    }

    for (const auto& [tableNode, indexDesc] : indexes) {
        if (useStreamIndex
                && (indexDesc->Type == TIndexDescription::EType::GlobalSync
                    || indexDesc->Type == TIndexDescription::EType::GlobalSyncUnique)) {
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
            if (inputColumnsSet.contains(column) && indexTableColumnsSet.emplace(column).second) {
                indexTableColumns.emplace_back(column);
            }
        }

        std::optional<TExprBase> upsertIndexRows;
        switch (indexDesc->Type) {
            case TIndexDescription::EType::GlobalAsync:
                AFL_ENSURE(false);
            case TIndexDescription::EType::GlobalSync:
            case TIndexDescription::EType::GlobalSyncUnique: {
                upsertIndexRows = MakeInsertIndexRows(*insertRows, table, inputColumnsSet, indexTableColumns,
                    insert.Pos(), ctx, true);
                break;
            }
            case TIndexDescription::EType::GlobalSyncVectorKMeansTree: {
                upsertIndexRows = MakeInsertIndexRows(*insertRows, table, inputColumnsSet, indexTableColumns,
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
            case TIndexDescription::EType::GlobalFulltextPlain:
            case TIndexDescription::EType::GlobalFulltextRelevance: {
                // For fulltext indexes, we need to tokenize the text and create inserted rows
                auto insertPrecompute = ReadInputToPrecompute(*insertRows, insert.Pos(), ctx);
                upsertIndexRows = BuildFulltextIndexRows(table, indexDesc, insertPrecompute, inputColumnsSet, indexTableColumns,
                    false /*forDelete*/, insert.Pos(), ctx);
                const auto* fulltextDesc = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&indexDesc->SpecializedIndexDescription);
                YQL_ENSURE(fulltextDesc);
                const bool withRelevance = fulltextDesc->GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE;
                if (withRelevance) {
                    // Update dictionary rows
                    const auto& dictTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << insert.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::DictTable);
                    auto dictRows = BuildFulltextDictRows(*upsertIndexRows, false /*useSum*/, true /*useStage*/, insert.Pos(), ctx);
                    effects.emplace_back(BuildFulltextDictUpsert(dictTable, dictRows, insert.Pos(), ctx));
                    // Insert document rows
                    const auto& docsTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << insert.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::DocsTable);
                    TVector<TStringBuf> docsColumns;
                    TExprBase docsRows = BuildFulltextDocsRows(table, indexDesc, insertPrecompute,
                        inputColumnsSet, docsColumns, false /*forDelete*/, insert.Pos(), ctx);
                    effects.emplace_back(Build<TKqlUpsertRows>(ctx, insert.Pos())
                        .Table(BuildTableMeta(docsTable, insert.Pos(), ctx))
                        .Input(docsRows)
                        .Columns(BuildColumnsList(docsColumns, insert.Pos(), ctx))
                        .ReturningColumns<TCoAtomList>().Build()
                        .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
                        .DefaultColumns<TCoAtomList>().Build()
                        .Done());
                    // Update statistics
                    const auto& statsTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << insert.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::StatsTable);
                    effects.emplace_back(BuildFulltextStatsUpsert(statsTable, docsRows, nullptr, insert.Pos(), ctx));
                }
                break;
            }
            case TIndexDescription::EType::LocalBloomFilter:
            case TIndexDescription::EType::LocalBloomNgramFilter:
                break;
        }
        Y_ENSURE(upsertIndexRows.has_value());
        Y_ENSURE(indexTableColumns);

        auto upsertIndex = Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(tableNode)
            .Input(upsertIndexRows.value())
            .Columns(BuildColumnsList(indexTableColumns, insert.Pos(), ctx))
            .ReturningColumns<TCoAtomList>().Build()
            .IsBatch(ctx.NewAtom(insert.Pos(), "false"))
            .DefaultColumns<TCoAtomList>().Build()
            .Done();

        effects.emplace_back(upsertIndex);
    }
    return Build<TExprList>(ctx, insert.Pos())
        .Add(effects)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
