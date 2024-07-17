#include "kqp_opt_log_impl.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/core/protos/config.pb.h>


namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

bool IsValidForRange(const NYql::TExprNode::TPtr& node) {
    TExprBase expr(node);
    if (auto sqlin = expr.Maybe<TCoSqlIn>()) {
        auto collection = sqlin.Cast().Collection().Ptr();
        bool result = true;
        VisitExpr(collection,
            [&](const TExprNode::TPtr& node) {
                if (node->IsCallable({"DqPhyPrecompute", "DqPrecompute"})) {
                    return false;
                }
                if (node->IsCallable() && (node->Content().StartsWith("Dq") || node->Content().StartsWith("Kql") || node->Content().StartsWith("Kqp"))) {
                    result = false;
                    return false;
                }
                return true;
            });
        return result;
    }

    return true;
}

} // namespace

TExprBase KqpPushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx, const NYql::TParentsMap& parentsMap)
{
    if (!node.Maybe<TCoFlatMapBase>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMapBase>();

    if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        return node;
    }

    auto readMatch = MatchRead<TKqlReadTableRangesBase>(flatmap.Input());
    if (!readMatch) {
        return node;
    }

    static const std::set<TStringBuf> supportedReads {
        TKqlReadTableRanges::CallableName(),
        TKqlReadTableIndexRanges::CallableName(),
    };

    if (!supportedReads.contains(readMatch->Read.Cast<TKqlReadTableRangesBase>().CallableName())) {
        return node;
    }

    if (readMatch->FlatMap) {
        return node;
    }

    auto read = readMatch->Read.Cast<TKqlReadTableRangesBase>();

    /*
     * ReadTableRanges supported predicate extraction, but it may be disabled via flag. For example to force
     * pushdown predicates to OLAP SSA program.
     */
    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();

    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return node;
    }

    if (!read.Ranges().Maybe<TCoVoid>()) {
        return node;
    }

    TMaybeNode<TCoAtom> indexName;
    if (auto maybeIndexRead = read.Maybe<TKqlReadTableIndexRanges>()) {
        indexName = maybeIndexRead.Cast().Index();
    }

    auto readSettings = TKqpReadTableSettings::Parse(read.Settings());
    const auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    THashSet<TString> possibleKeys;
    TPredicateExtractorSettings settings;
    settings.MergeAdjacentPointRanges = true;
    settings.HaveNextValueCallable = true;
    settings.BuildLiteralRange = true;
    settings.IsValidForRange = IsValidForRange;

    if (kqpCtx.Config->ExtractPredicateRangesLimit != 0) {
        settings.MaxRanges = kqpCtx.Config->ExtractPredicateRangesLimit;
    } else {
        settings.MaxRanges = Nothing();
    }

    auto extractor = MakePredicateRangeExtractor(settings);
    YQL_ENSURE(mainTableDesc.SchemeNode);

    bool prepareSuccess = extractor->Prepare(flatmap.Lambda().Ptr(), *mainTableDesc.SchemeNode, possibleKeys, ctx, typesCtx);
    YQL_ENSURE(prepareSuccess);

    if (!indexName.IsValid() && !readSettings.ForcePrimary && kqpCtx.Config->IndexAutoChooserMode != NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_DISABLED) {
        using TIndexComparisonKey = std::tuple<bool, size_t, bool, size_t, bool>;
        auto calcNeedsJoin = [&] (const TKikimrTableMetadataPtr& keyTable) -> bool {
            bool needsJoin = false;
            for (auto&& column : read.Columns()) {
                if (!keyTable->Columns.contains(column.Value())) {
                    needsJoin = true;
                }
            }
            return needsJoin;
        };

        auto calcKey = [&](NYql::IPredicateRangeExtractor::TBuildResult buildResult, size_t descriptionKeyColumns, bool needsJoin) -> TIndexComparisonKey {
            return std::make_tuple(
                buildResult.PointPrefixLen >= descriptionKeyColumns,
                buildResult.PointPrefixLen >= descriptionKeyColumns ? 0 : buildResult.PointPrefixLen,
                buildResult.UsedPrefixLen >= descriptionKeyColumns,
                buildResult.UsedPrefixLen >= descriptionKeyColumns ? 0 : buildResult.UsedPrefixLen,
                !needsJoin);
        };

        TMaybe<TString> chosenIndex;
        auto primaryBuildResult = extractor->BuildComputeNode(mainTableDesc.Metadata->KeyColumnNames, ctx, typesCtx);

        if (primaryBuildResult.PointPrefixLen < mainTableDesc.Metadata->KeyColumnNames.size()) {
            auto maxKey = calcKey(primaryBuildResult, mainTableDesc.Metadata->KeyColumnNames.size(), false);
            for (auto& index : mainTableDesc.Metadata->Indexes) {
                if (index.Type != TIndexDescription::EType::GlobalAsync) {
                    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, mainTableDesc.Metadata->GetIndexMetadata(TString(index.Name)).first->Name);
                    auto buildResult = extractor->BuildComputeNode(tableDesc.Metadata->KeyColumnNames, ctx, typesCtx);
                    bool needsJoin = calcNeedsJoin(tableDesc.Metadata);

                    if (needsJoin && kqpCtx.Config->IndexAutoChooserMode == NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_ONLY_FULL_KEY && buildResult.PointPrefixLen < index.KeyColumns.size()) {
                        continue;
                    }
                    if (needsJoin && kqpCtx.Config->IndexAutoChooserMode == NKikimrConfig::TTableServiceConfig_EIndexAutoChooseMode_ONLY_POINTS && buildResult.PointPrefixLen == 0) {
                        continue;
                    }

                    auto key = calcKey(buildResult, index.KeyColumns.size(), needsJoin);
                    if (key > maxKey) {
                        maxKey = key;
                        chosenIndex = index.Name;
                    }
                }
            }
        }

        if (chosenIndex) {
            indexName = ctx.NewAtom(read.Pos(), *chosenIndex);
        }
    }

    auto& tableDesc = indexName ? kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, mainTableDesc.Metadata->GetIndexMetadata(TString(indexName.Cast())).first->Name) : mainTableDesc;

    auto buildResult = extractor->BuildComputeNode(tableDesc.Metadata->KeyColumnNames, ctx, typesCtx);

    TExprNode::TPtr ranges = buildResult.ComputeNode;

    if (!ranges) {
        return node;
    }

    TExprNode::TPtr prefixPointsExpr;
    IPredicateRangeExtractor::TBuildResult pointsExtractionResult;

    if (buildResult.PointPrefixLen > 0) {
        TPredicateExtractorSettings pointSettings = settings;
        pointSettings.MergeAdjacentPointRanges = false;
        pointSettings.HaveNextValueCallable = false;
        pointSettings.MaxRanges = Nothing();
        TVector<TString> pointKeys;
        for (size_t i = 0; i < buildResult.PointPrefixLen; ++i) {
            pointKeys.push_back(tableDesc.Metadata->KeyColumnNames[i]);
        }
        auto extractor = MakePredicateRangeExtractor(pointSettings);
        YQL_ENSURE(extractor->Prepare(flatmap.Lambda().Ptr(), *mainTableDesc.SchemeNode, possibleKeys, ctx, typesCtx));

        pointsExtractionResult = extractor->BuildComputeNode(pointKeys, ctx, typesCtx);
        YQL_ENSURE(pointsExtractionResult.ComputeNode);
        prefixPointsExpr = BuildPointsList(pointsExtractionResult, pointKeys, ctx);
        YQL_CLOG(DEBUG, ProviderKqp) << "Points extracted: " << KqpExprToPrettyString(*prefixPointsExpr, ctx);
        YQL_CLOG(DEBUG, ProviderKqp) << "Residual lambda: " << KqpExprToPrettyString(*pointsExtractionResult.PrunedLambda, ctx);
    }

    TExprNode::TPtr residualLambda = buildResult.PrunedLambda;

    TVector<TString> usedColumns;
    usedColumns.reserve(buildResult.UsedPrefixLen);

    for (size_t i = 0; i < buildResult.UsedPrefixLen; ++i) {
        usedColumns.emplace_back(tableDesc.Metadata->KeyColumnNames[i]);
    }

    TKqpReadTableExplainPrompt prompt;
    prompt.SetUsedKeyColumns(usedColumns);
    if (buildResult.ExpectedMaxRanges.Defined()) {
        prompt.SetExpectedMaxRanges(buildResult.ExpectedMaxRanges.GetRef());
    }
    prompt.SetPointPrefixLen(buildResult.PointPrefixLen);

    YQL_CLOG(DEBUG, ProviderKqp) << "Ranges extracted: " << KqpExprToPrettyString(*ranges, ctx);
    YQL_CLOG(DEBUG, ProviderKqp) << "Residual lambda: " << KqpExprToPrettyString(*residualLambda, ctx);

    TMaybe<TExprBase> input;
    if ((tableDesc.Metadata->Kind == EKikimrTableKind::Datashard ||
         tableDesc.Metadata->Kind == EKikimrTableKind::SysView))
    {
            auto buildLookup = [&] (TExprNode::TPtr keys, TMaybe<TExprBase>& result) {
                if (indexName) {
                    if (kqpCtx.IsScanQuery()) {
                        if (kqpCtx.Config->EnableKqpScanQueryStreamLookup) {
                            result = Build<TKqlStreamLookupIndex>(ctx, node.Pos())
                                .Table(read.Table())
                                .Columns(read.Columns())
                                .LookupKeys(keys)
                                .Index(indexName.Cast())
                                .LookupKeys(keys)
                                .Done();
                        }
                    } else {
                        result = Build<TKqlLookupIndex>(ctx, node.Pos())
                            .Table(read.Table())
                            .Columns(read.Columns())
                            .LookupKeys(keys)
                            .Index(indexName.Cast())
                            .Done();
                    }
                } else {
                    if (kqpCtx.IsScanQuery()) {
                        if (kqpCtx.Config->EnableKqpScanQueryStreamLookup) {
                            result = Build<TKqlStreamLookupTable>(ctx, node.Pos())
                                .Table(read.Table())
                                .Columns(read.Columns())
                                .LookupKeys(keys)
                                .LookupStrategy().Build(TKqpStreamLookupStrategyName)
                                .Done();
                        }
                    } else {
                        result = Build<TKqlLookupTable>(ctx, node.Pos())
                            .Table(read.Table())
                            .Columns(read.Columns())
                            .LookupKeys(keys)
                            .Done();
                    }
                }
            };

        if (buildResult.LiteralRange) {
            bool ispoint = buildResult.PointPrefixLen == tableDesc.Metadata->KeyColumnNames.size();
            if (ispoint && tableDesc.Metadata->Kind != EKikimrTableKind::SysView) {
                TVector<TExprBase> structMembers;
                for (size_t i = 0; i < tableDesc.Metadata->KeyColumnNames.size(); ++i) {
                    auto member = Build<TCoNameValueTuple>(ctx, node.Pos())
                        .Name().Build(tableDesc.Metadata->KeyColumnNames[i])
                        .Value(buildResult.LiteralRange->Left.Columns[i])
                        .Done();

                    structMembers.push_back(member);
                }
                TExprBase keys = Build<TCoAsList>(ctx, node.Pos())
                    .Add<TCoAsStruct>()
                        .Add(structMembers)
                        .Build()
                    .Done();

                buildLookup(keys.Ptr(), input);
            } else {
                auto fromExpr = buildResult.LiteralRange->Left.Inclusive
                    ? Build<TKqlKeyInc>(ctx, read.Pos()).Add(buildResult.LiteralRange->Left.Columns).Done().Cast<TKqlKeyTuple>()
                    : Build<TKqlKeyExc>(ctx, read.Pos()).Add(buildResult.LiteralRange->Left.Columns).Done().Cast<TKqlKeyTuple>();

                auto toExpr = buildResult.LiteralRange->Right.Inclusive
                    ? Build<TKqlKeyInc>(ctx, read.Pos()).Add(buildResult.LiteralRange->Right.Columns).Done().Cast<TKqlKeyTuple>()
                    : Build<TKqlKeyExc>(ctx, read.Pos()).Add(buildResult.LiteralRange->Right.Columns).Done().Cast<TKqlKeyTuple>();

                auto keyRange = Build<TKqlKeyRange>(ctx, read.Pos())
                    .From(fromExpr)
                    .To(toExpr)
                    .Done();

                if (indexName) {
                    input = Build<TKqlReadTableIndex>(ctx, read.Pos())
                        .Table(read.Table())
                        .Columns(read.Columns())
                        .Settings(read.Settings())
                        .Range(keyRange)
                        .Index(indexName.Cast())
                        .Done();
                } else {
                    input = Build<TKqlReadTable>(ctx, read.Pos())
                        .Table(read.Table())
                        .Columns(read.Columns())
                        .Settings(read.Settings())
                        .Range(keyRange)
                        .Done();
                }
            }
        }
    }

    if (!input) {
        TMaybeNode<TExprBase> prefix;
        TMaybeNode<TCoLambda> predicateExpr;
        TMaybeNode<TCoAtomList> usedColumnsList;
        prefix = prefixPointsExpr;
        if (prefix) {
            predicateExpr = ctx.DeepCopyLambda(flatmap.Lambda().Ref());
            TSet<TString> usedColumns;
            if (!ExtractUsedFields(
                flatmap.Lambda().Body().Ptr(),
                flatmap.Lambda().Args().Arg(0).Ref(),
                usedColumns,
                parentsMap,
                true))
            {
                prefix = {};
                predicateExpr = {};
            } else {
                TVector<TCoAtom> columnAtoms;
                for (auto&& column : usedColumns) {
                    columnAtoms.push_back(Build<TCoAtom>(ctx, read.Pos()).Value(column).Done());
                }
                usedColumnsList = Build<TCoAtomList>(ctx, read.Pos()).Add(columnAtoms).Done();
            }
        }


        if (indexName) {
            input = Build<TKqlReadTableIndexRanges>(ctx, read.Pos())
                .Table(read.Table())
                .Ranges(ranges)
                .Columns(read.Columns())
                .Settings(read.Settings())
                .ExplainPrompt(prompt.BuildNode(ctx, read.Pos()))
                .Index(indexName.Cast())
                .PrefixPointsExpr(prefix)
                .PredicateExpr(predicateExpr)
                .PredicateUsedColumns(usedColumnsList)
                .Done();
        } else {
            input = Build<TKqlReadTableRanges>(ctx, read.Pos())
                .Table(read.Table())
                .Ranges(ranges)
                .Columns(read.Columns())
                .Settings(read.Settings())
                .ExplainPrompt(prompt.BuildNode(ctx, read.Pos()))
                .PrefixPointsExpr(prefix)
                .PredicateExpr(predicateExpr)
                .PredicateUsedColumns(usedColumnsList)
                .Done();
        }
    }

    *input = readMatch->BuildProcessNodes(*input, ctx);
    if (node.Maybe<TCoFlatMap>()) {
        return Build<TCoFlatMap>(ctx, node.Pos())
            .Input(*input)
            .Lambda(residualLambda)
            .Done();
    } else {
        return Build<TCoOrderedFlatMap>(ctx, node.Pos())
            .Input(*input)
            .Lambda(residualLambda)
            .Done();
    }
}

} // namespace NKikimr::NKqp::NOpt

