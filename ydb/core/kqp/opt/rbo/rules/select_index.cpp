#include "kqp_rbo_range_extraction.h"

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

#include <tuple>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp::NRangeExtraction;

/**
 * How well a key order suits the filter of the read. Compared lexicographically, the same way the
 * old optimizer ranks index candidates: an exact point prefix over the whole key beats everything,
 * then longer point prefixes, then range prefixes. Prefix lengths are zeroed when they span the
 * whole key so that the "covers the key" flags dominate a longer but partial prefix.
 */
struct TIndexScore {
    bool PointCoversKey = false;
    size_t PointPrefixLen = 0;
    bool UsedCoversKey = false;
    size_t UsedPrefixLen = 0;

    std::tuple<bool, size_t, bool, size_t> AsTuple() const {
        return std::make_tuple(PointCoversKey, PointPrefixLen, UsedCoversKey, UsedPrefixLen);
    }

    bool operator<(const TIndexScore& other) const { return AsTuple() < other.AsTuple(); }
};

TIndexScore ScoreKeyOrder(const IPredicateRangeExtractor::TBuildResult& result, size_t keyLen) {
    TIndexScore score;
    score.PointCoversKey = keyLen != 0 && result.PointPrefixLen == keyLen;
    score.PointPrefixLen = score.PointCoversKey ? 0 : result.PointPrefixLen;
    score.UsedCoversKey = keyLen != 0 && result.UsedPrefixLen == keyLen;
    score.UsedPrefixLen = score.UsedCoversKey ? 0 : result.UsedPrefixLen;
    return score;
}

bool IsSelectableIndex(const TIndexDescription& index) {
    if (index.State != TIndexDescription::EIndexState::Ready) {
        return false;
    }
    switch (index.Type) {
        case TIndexDescription::EType::GlobalSync:
        case TIndexDescription::EType::GlobalSyncUnique:
            return true;
        default:
            // Async indexes are not consistent with the main table, all the other kinds are either
            // local (no impl table to read) or need a specialized read operator.
            return false;
    }
}

// Every column the read produces must be present in the index impl table, otherwise the missing
// ones would have to be fetched from the main table by a lookup (not supported yet).
bool IsCovering(const TOpRead& read, const TKikimrTableMetadata& indexMeta) {
    for (const auto& column : read.Columns) {
        if (!indexMeta.Columns.contains(column)) {
            return false;
        }
    }
    return true;
}

// An index whose impl table starts with the main table key in the same order gives the same scan
// order as the main table, so reading it can never win.
bool IsUselessIndex(const TVector<TString>& indexKeyColumns, const TVector<TString>& mainKeyColumns) {
    const size_t common = std::min(indexKeyColumns.size(), mainKeyColumns.size());
    for (size_t i = 0; i < common; ++i) {
        if (indexKeyColumns[i] != mainKeyColumns[i]) {
            return false;
        }
    }
    return true;
}

const TKikimrTableDescription* FindTable(const NOpt::TKqpOptimizeContext& kqpCtx, const TString& path) {
    const auto& tables = kqpCtx.Tables->GetTables();
    const auto it = tables.find(std::make_pair(kqpCtx.Cluster, path));
    return it != tables.end() ? &it->second : nullptr;
}

NYql::EStorageType GetStorageType(const TKikimrTableMetadata& meta) {
    switch (meta.Kind) {
        case EKikimrTableKind::Datashard:
            return NYql::EStorageType::RowStorage;
        case EKikimrTableKind::Olap:
            return NYql::EStorageType::ColumnStorage;
        default:
            return NYql::EStorageType::NA;
    }
}

TExprNode::TPtr BuildTableCallable(const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) {
    // BuildTableMeta from ydb/core/kqp/opt cannot be reused here: rbo is a dependency of that
    // library, so peerdir'ing it back would close a cycle.
    // clang-format off
    return Build<TKqpTable>(ctx, pos)
        .Path().Build(meta.Name)
        .PathId().Build(meta.PathId.ToString())
        .SysView().Build(meta.SysView)
        .Version().Build(meta.SchemaVersion)
    .Done().Ptr();
    // clang-format on
}

} // anonymous namespace

bool TSelectIndexRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter && input->Children.front()->Kind == EOperator::Source;
}

/**
 * Automatic secondary index selection: point a filtered read at the impl table of the index that
 * suits the filter best, as long as that index covers everything the read produces.
 */
TIntrusivePtr<IOperator> TSelectIndexRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    if (kqpCtx.Config->IsAutoIndexSelectionDisabled() || kqpCtx.Config->GetOptPredicateExtract() == EOptionalFlag::Disabled) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto filterType = filter->FilterExpr.Node->GetTypeAnn();
    if (!filterType || filterType->GetKind() == ETypeAnnotationKind::Pg) {
        return input;
    }

    const auto read = CastOperator<TOpRead>(filter->GetInput());
    if (read->GetRanges()) {
        // Ranges are built for the key order of the current table, retargeting would invalidate them.
        return input;
    }

    const auto tablePath = TExprBase(read->GetTable()).Cast<TKqpTable>().Path().StringValue();
    const auto tableDesc = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx);
    if (!tableDesc || !tableDesc->Metadata || tableDesc->Metadata->Indexes.empty()) {
        return input;
    }

    const auto& mainMeta = *tableDesc->Metadata;
    auto lambda = TCoLambda(GetLambdaForRangeExtractor(filter->FilterExpr.Node, read->Type, rboCtx));
    // Predicate extract lib requires constraints.
    lambda.Args().Arg(0).Ptr()->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    THashSet<TString> possibleKeys;
    auto extractor = MakePredicateRangeExtractor(PrepareExtractorSettings(kqpCtx));
    const auto schemeType = PrepareSchemeType(*read, tableDesc->SchemeNode, ctx);
    if (!extractor->Prepare(lambda.Ptr(), *schemeType, possibleKeys, ctx, typeCtx)) {
        return input;
    }

    const auto mainKeyColumns = ResolveExposedKeyColumns(*read, mainMeta.KeyColumnNames);
    const auto mainResult = extractor->BuildComputeNode(mainKeyColumns, ctx, typeCtx);
    auto bestScore = ScoreKeyOrder(mainResult, mainKeyColumns.size());
    if (bestScore.PointCoversKey) {
        // A point lookup by the whole primary key cannot be beaten by any index.
        return input;
    }

    TIntrusivePtr<TKikimrTableMetadata> chosenIndexMeta;
    for (const auto& index : mainMeta.Indexes) {
        if (!IsSelectableIndex(index)) {
            continue;
        }

        const auto indexMeta = mainMeta.GetIndexMetadata(index.Name).first;
        if (!indexMeta || IsUselessIndex(indexMeta->KeyColumnNames, mainMeta.KeyColumnNames) || !IsCovering(*read, *indexMeta)) {
            continue;
        }

        // The impl table must have been loaded together with the main table, otherwise the read
        // could not be type annotated later on.
        if (!FindTable(kqpCtx, indexMeta->Name)) {
            continue;
        }

        const auto indexKeyColumns = ResolveExposedKeyColumns(*read, indexMeta->KeyColumnNames);
        const auto indexResult = extractor->BuildComputeNode(indexKeyColumns, ctx, typeCtx);
        if (!indexResult.ComputeNode) {
            continue;
        }

        const auto score = ScoreKeyOrder(indexResult, indexKeyColumns.size());
        if (bestScore < score) {
            bestScore = score;
            chosenIndexMeta = indexMeta;
        }
    }

    if (!chosenIndexMeta) {
        return input;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Selected index " << chosenIndexMeta->Name << " for a read of " << tablePath;

    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), GetStorageType(*chosenIndexMeta),
                                          BuildTableCallable(*chosenIndexMeta, read->Pos, ctx), read->OlapFilterLambda, read->Limit,
                                          std::nullopt, read->OriginalPredicate, read->SortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, filter->FilterExpr);
}

} // namespace NKikimr::NKqp
