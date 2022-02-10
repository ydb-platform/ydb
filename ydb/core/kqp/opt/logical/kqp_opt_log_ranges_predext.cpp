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
 
    TMaybeNode<TKqlReadTableRanges> readTable; 
    TMaybeNode<TCoFilterNullMembers> filterNull; 
    TMaybeNode<TCoSkipNullMembers> skipNull; 
 
    if (auto maybeRead = flatmap.Input().Maybe<TKqlReadTableRanges>()) { 
        readTable = maybeRead.Cast(); 
    } 
 
    if (auto maybeRead = flatmap.Input().Maybe<TCoFilterNullMembers>().Input().Maybe<TKqlReadTableRanges>()) { 
        readTable = maybeRead.Cast(); 
        filterNull = flatmap.Input().Cast<TCoFilterNullMembers>(); 
    } 
 
    if (auto maybeRead = flatmap.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlReadTableRanges>()) { 
        readTable = maybeRead.Cast(); 
        skipNull = flatmap.Input().Cast<TCoSkipNullMembers>(); 
    } 
 
    if (!readTable) { 
        return node; 
    } 
 
    /* 
     * ReadTableRanges supported predicate extraction, but it may be disabled via flag. For example to force 
     * pushdown predicates to OLAP SSA program. 
     */ 
    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract(); 
 
    if (predicateExtractSetting == EOptionalFlag::Disabled) { 
        return node; 
    } 
 
    auto read = readTable.Cast(); 
 
    if (!read.Ranges().Maybe<TCoVoid>()) { 
        return node; 
    } 
 
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path()); 
 
    THashSet<TString> possibleKeys; 
    TPredicateExtractorSettings settings;
    settings.MergeAdjacentPointRanges = true; 
    auto extractor = MakePredicateRangeExtractor(settings);
    YQL_ENSURE(tableDesc.SchemeNode);
 
    bool prepareSuccess = extractor->Prepare(flatmap.Lambda().Ptr(), *tableDesc.SchemeNode, possibleKeys, ctx, typesCtx);
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
 
    TMaybeNode<TExprBase> readInput = Build<TKqlReadTableRanges>(ctx, read.Pos()) 
        .Table(read.Table()) 
        .Ranges(ranges) 
        .Columns(read.Columns()) 
        .Settings(read.Settings()) 
        .ExplainPrompt(prompt.BuildNode(ctx, read.Pos())) 
        .Done(); 
 
    auto input = readInput.Cast(); 
 
    if (filterNull) { 
        input = Build<TCoFilterNullMembers>(ctx, node.Pos()) 
            .Input(input) 
            .Members(filterNull.Cast().Members()) 
            .Done(); 
    } 
 
    if (skipNull) { 
        input = Build<TCoSkipNullMembers>(ctx, node.Pos()) 
            .Input(input) 
            .Members(skipNull.Cast().Members()) 
            .Done(); 
    } 
 
    return Build<TCoFlatMap>(ctx, node.Pos()) 
        .Input(input) 
        .Lambda(residualLambda) 
        .Done(); 
} 
 
} // namespace NKikimr::NKqp::NOpt
 
