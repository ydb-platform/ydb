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

TExprBase BuildDeleteIndexStagesImpl(const TKikimrTableDescription& table,
    const TSecondaryIndexes& indexes, const TKqlDeleteRowsIndex& del,
    const TExprBase& lookupKeys, std::function<TExprBase(const TVector<TStringBuf>&)> project,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    const auto& pk = table.Metadata->KeyColumnNames;

    auto tableDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
        .Table(del.Table())
        .Input(lookupKeys)
        .ReturningColumns(del.ReturningColumns())
        .IsBatch(del.IsBatch())
        .Done();

    TVector<TExprBase> effects;
    effects.emplace_back(tableDelete);

    const bool isSink = NeedSinks(table, kqpCtx);
    const bool useStreamIndex = isSink && kqpCtx.Config->GetEnableIndexStreamWrite();

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

        for (const auto& column : pk) {
            if (indexTableColumnsSet.insert(column).second) {
                indexTableColumns.emplace_back(column);
            }
        }

        auto deleteIndexKeys = project(indexTableColumns);

        switch (indexDesc->Type) {
            case TIndexDescription::EType::GlobalAsync:
                AFL_ENSURE(false);
            case TIndexDescription::EType::GlobalSync:
            case TIndexDescription::EType::GlobalSyncUnique: {
                // deleteIndexKeys are already correct
                break;
            }
            case TIndexDescription::EType::GlobalSyncVectorKMeansTree: {
                if (indexDesc->KeyColumns.size() > 1) {
                    const auto& prefixTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << del.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NKMeans::PrefixTable);
                    deleteIndexKeys = BuildVectorIndexPrefixRows(table, prefixTable, false, indexDesc, deleteIndexKeys, indexTableColumns, del.Pos(), ctx);
                }
                deleteIndexKeys = BuildVectorIndexPostingRows(table, del.Table(), indexDesc->Name,
                    indexTableColumns, deleteIndexKeys, false, del.Pos(), ctx);
                break;
            }
            case TIndexDescription::EType::GlobalFulltextPlain:
            case TIndexDescription::EType::GlobalFulltextRelevance: {
                // For fulltext indexes, we need to tokenize the text and create deleted rows
                const auto deletePrecompute = ReadInputToPrecompute(deleteIndexKeys, del.Pos(), ctx);
                deleteIndexKeys = BuildFulltextIndexRows(table, indexDesc, deletePrecompute, indexTableColumnsSet, indexTableColumns,
                    true /*forDelete*/, del.Pos(), ctx);
                const auto* fulltextDesc = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&indexDesc->SpecializedIndexDescription);
                YQL_ENSURE(fulltextDesc);
                const bool withRelevance = fulltextDesc->GetSettings().layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE;
                if (withRelevance) {
                    // Update dictionary rows
                    const auto& dictTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << del.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::DictTable);
                    auto dictRows = BuildFulltextDictRows(deleteIndexKeys, false /*useSum*/, true /*useStage*/, del.Pos(), ctx);
                    effects.emplace_back(BuildFulltextDictUpsert(dictTable, dictRows, del.Pos(), ctx));
                    // Rows in deleteIndexKeys include __ydb_freq, but we don't need it for delete keys
                    deleteIndexKeys = BuildFulltextPostingKeys(table, deleteIndexKeys, del.Pos(), ctx);
                    // Delete document rows
                    const auto& docsTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << del.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::DocsTable);
                    auto docsKeys = project(TVector<TStringBuf>(pk.begin(), pk.end())); // TVector<TString> to TVector<TStringBuf>
                    effects.emplace_back(Build<TKqlDeleteRows>(ctx, del.Pos())
                        .Table(BuildTableMeta(docsTable, del.Pos(), ctx))
                        .Input(docsKeys)
                        .ReturningColumns<TCoAtomList>().Build()
                        .IsBatch(ctx.NewAtom(del.Pos(), "false"))
                        .Done());
                    // Update statistics
                    const auto& statsTable = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, TStringBuilder() << del.Table().Path().Value()
                        << "/" << indexDesc->Name << "/" << NKikimr::NTableIndex::NFulltext::StatsTable);
                    TVector<TStringBuf> docsColumns;
                    auto docsRows = BuildFulltextDocsRows(table, indexDesc, deletePrecompute,
                        indexTableColumnsSet, docsColumns, true /*forDelete*/, del.Pos(), ctx);
                    effects.emplace_back(BuildFulltextStatsUpsert(statsTable, nullptr, docsRows, del.Pos(), ctx));
                }
                break;
            }
        }

        auto indexDelete = Build<TKqlDeleteRows>(ctx, del.Pos())
            .Table(tableNode)
            .Input(deleteIndexKeys)
            .ReturningColumns<TCoAtomList>().Build()
            .IsBatch(ctx.NewAtom(del.Pos(), "false"))
            .Done();
        effects.emplace_back(std::move(indexDelete));
    }

    return Build<TExprList>(ctx, del.Pos())
        .Add(effects)
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

    const auto indexes = BuildAffectedIndexTables(table, del.Pos(), ctx);
    YQL_ENSURE(indexes);

    // Skip lookup means that the input already has all required columns and we only need to project them
    auto settings = TKqpDeleteRowsIndexSettings::Parse(del);

    if (settings.SkipLookup) {
        auto lookupKeys = ProjectColumns(del.Input(), pk, ctx);
        return BuildDeleteIndexStagesImpl(table, indexes, del, lookupKeys, [&](const TVector<TStringBuf>& indexTableColumns) {
            return ProjectColumns(del.Input(), indexTableColumns, ctx);
        }, ctx, kqpCtx);
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
    }, ctx, kqpCtx);
}

} // namespace NKikimr::NKqp::NOpt
