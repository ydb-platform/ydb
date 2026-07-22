#pragma once

#include <ydb/core/kqp/opt/rbo/kqp_operator.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_context.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

/**
 * Helpers shared by the rules that run the predicate range extractor over a table read:
 * range pushdown ("Push ranges") and secondary index selection ("Select indexes").
 */
namespace NKikimr::NKqp::NRangeExtraction {

NYql::TPredicateExtractorSettings PrepareExtractorSettings(NOpt::TKqpOptimizeContext& kqpCtx);

// Prepare the filter lambda for the extract predicate library: coalesce an optional predicate to
// false and run peephole, so the lambda only contains callables the extractor understands.
TExprNode::TPtr GetLambdaForRangeExtractor(TExprNode::TPtr node, const TTypeAnnotationNode* inputType, TRBOContext& rboCtx);

// Rewrite the table scheme type so its member names match the names the read exposes (the
// extractor is prepared against the filter lambda, which references exposed names).
const TStructExprType* PrepareSchemeType(const TOpRead& read, const TStructExprType* schemeType, TExprContext& ctx);

// Translate physical key column names of some table (the read's own table or an index impl table)
// into the names the read exposes, matching PrepareSchemeType.
TVector<TString> ResolveExposedKeyColumns(const TOpRead& read, const TVector<TString>& physicalKeyColumns);

} // namespace NKikimr::NKqp::NRangeExtraction
