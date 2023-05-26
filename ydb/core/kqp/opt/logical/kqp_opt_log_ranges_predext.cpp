#include "kqp_opt_log_impl.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/providers/common/provider/yql_table_lookup.h>
#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TMaybeNode<TExprBase> TryBuildTrivialReadTable(TCoFlatMap& flatmap, TKqlReadTableRangesBase readTable,
    const TKqpMatchReadResult& readMatch, const TKikimrTableDescription& tableDesc, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx, TMaybeNode<TCoAtom> indexName)
{
    Y_UNUSED(kqpCtx);

    switch (tableDesc.Metadata->Kind) {
        case EKikimrTableKind::Datashard:
        case EKikimrTableKind::SysView:
            break;
        case EKikimrTableKind::Olap:
        case EKikimrTableKind::External:
        case EKikimrTableKind::Unspecified:
            return {};
    }

    auto row = flatmap.Lambda().Args().Arg(0);
    auto predicate = TExprBase(flatmap.Lambda().Body().Ref().ChildPtr(0));
    TTableLookup lookup = ExtractTableLookup(row, predicate, tableDesc.Metadata->KeyColumnNames,
        &KqpTableLookupGetValue, &KqpTableLookupCanCompare, &KqpTableLookupCompare, ctx, false);

    if (lookup.IsFullScan()) {
        return {};
    }

    if (lookup.GetKeyRanges().size() > 1) {
        return {}; // optimize trivial cases only
    }

    auto isTrivialExpr = [](const TExprBase& expr) {
        if (!expr.Maybe<TCoExists>()) {
            return false;
        }
        auto opt = expr.Cast<TCoExists>().Optional();
        if (opt.Maybe<TCoDataCtor>()) {
            return true;
        }
        if (opt.Maybe<TCoParameter>()) {
            return opt.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Data;
        }
        return false;
    };

    auto isTrivialPredicate = [&isTrivialExpr](const TExprBase& expr) {
        if (isTrivialExpr(expr)) {
            return true;
        }
        if (expr.Maybe<TCoAnd>()) {
            for (auto& predicate : expr.Cast<TCoAnd>().Args()) {
                if (!isTrivialExpr(TExprBase(predicate))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    };

    TVector<TExprBase> fetches;
    fetches.reserve(lookup.GetKeyRanges().size());
    auto readSettings = TKqpReadTableSettings::Parse(readTable);

    for (const auto& keyRange : lookup.GetKeyRanges()) {
        if (keyRange.HasResidualPredicate()) {
            // In trivial cases the residual predicate look like:
            //  * (Exists <KeyValue>)
            //  * (And (Exists <Key1Value>) (Exists Key2Value) ...)
            // where `KeyValue` is either explicit `Data` (so `Exists` is always true)
            //   or Parameter value (in that case we ensure that type is not optional)
            if (!isTrivialPredicate(keyRange.GetResidualPredicate().Cast())) {
                return {};
            }
        }

        auto keyRangeExpr = BuildKeyRangeExpr(keyRange, tableDesc, readTable.Pos(), ctx);

        TKqpReadTableSettings settings = readSettings;
        for (size_t i = 0; i < keyRange.GetColumnRangesCount(); ++i) {
            const auto& column = tableDesc.Metadata->KeyColumnNames[i];
            auto& range = keyRange.GetColumnRange(i);
            if (range.IsDefined() && !range.IsNull()) {
                settings.AddSkipNullKey(column);
            }
        }

        auto buildReadTable = [&] () -> TExprBase {
            return Build<TKqlReadTable>(ctx, readTable.Pos())
                .Table(readTable.Table())
                .Range(keyRangeExpr)
                .Columns(readTable.Columns())
                .Settings(settings.BuildNode(ctx, readTable.Pos()))
                .Done();
        };

        auto buildReadIndex = [&] () -> TExprBase {
            return Build<TKqlReadTableIndex>(ctx, readTable.Pos())
                .Table(readTable.Table())
                .Range(keyRangeExpr)
                .Columns(readTable.Columns())
                .Settings(settings.BuildNode(ctx, readTable.Pos()))
                .Index(indexName.Cast())
                .Done();
        };

        TExprBase input = indexName.IsValid() ? buildReadIndex() : buildReadTable();

        input = readMatch.BuildProcessNodes(input, ctx);

        input = Build<TCoFlatMap>(ctx, readTable.Pos())
            .Input(input)
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(TExprBase(ctx.ChangeChild(flatmap.Lambda().Body().Ref(), 0, MakeBool<true>(readTable.Pos(), ctx))))
                    .With(flatmap.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done();

        if (lookup.GetKeyRanges().size() == 1) {
            return input;
        }

        fetches.emplace_back(input);
    }

    return Build<TCoExtend>(ctx, readTable.Pos())
        .Add(fetches)
        .Done();
}

} // namespace

TExprBase KqpPushExtractedPredicateToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TTypeAnnotationContext& typesCtx)
{
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }

    auto flatmap = node.Cast<TCoFlatMap>();

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

    const auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());
    auto& tableDesc = indexName ? kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, mainTableDesc.Metadata->GetIndexMetadata(TString(indexName.Cast())).first->Name) : mainTableDesc;

    // test for trivial cases (explicit literals or parameters)
    if (auto expr = TryBuildTrivialReadTable(flatmap, read, *readMatch, tableDesc, ctx, kqpCtx, indexName)) {
        return expr.Cast();
    }

    THashSet<TString> possibleKeys;
    TPredicateExtractorSettings settings;
    settings.MergeAdjacentPointRanges = true;
    auto extractor = MakePredicateRangeExtractor(settings);
    YQL_ENSURE(tableDesc.SchemeNode);

    bool prepareSuccess = extractor->Prepare(flatmap.Lambda().Ptr(), *mainTableDesc.SchemeNode, possibleKeys, ctx, typesCtx);
    YQL_ENSURE(prepareSuccess);

    auto buildResult = extractor->BuildComputeNode(tableDesc.Metadata->KeyColumnNames, ctx);
    TExprNode::TPtr ranges = buildResult.ComputeNode;

    if (!ranges) {
        return node;
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

    YQL_CLOG(DEBUG, ProviderKqp) << "Ranges extracted: " << KqpExprToPrettyString(*ranges, ctx);
    YQL_CLOG(DEBUG, ProviderKqp) << "Residual lambda: " << KqpExprToPrettyString(*residualLambda, ctx);

    TMaybe<TExprBase> input;
    if (indexName) {
        input = Build<TKqlReadTableIndexRanges>(ctx, read.Pos())
            .Table(read.Table())
            .Ranges(ranges)
            .Columns(read.Columns())
            .Settings(read.Settings())
            .ExplainPrompt(prompt.BuildNode(ctx, read.Pos()))
            .Index(indexName.Cast())
            .Done();
    } else {
        input = Build<TKqlReadTableRanges>(ctx, read.Pos())
            .Table(read.Table())
            .Ranges(ranges)
            .Columns(read.Columns())
            .Settings(read.Settings())
            .ExplainPrompt(prompt.BuildNode(ctx, read.Pos()))
            .Done();
    }

    *input = readMatch->BuildProcessNodes(*input, ctx);

    return Build<TCoFlatMap>(ctx, node.Pos())
        .Input(*input)
        .Lambda(residualLambda)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

