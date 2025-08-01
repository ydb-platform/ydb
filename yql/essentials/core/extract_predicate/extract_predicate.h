#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>

namespace NYql {

struct TPredicateExtractorSettings {
    TMaybe<size_t> MaxRanges = 10000; // should be less than Max<size_t>() due to integer overflow
    bool MergeAdjacentPointRanges = true;
    bool HaveNextValueCallable = false;
    bool BuildLiteralRange = false;
    std::function<bool(const NYql::TExprNode::TPtr&)> IsValidForRange;
    // Specify the external parameter limits to ensure better ExpectedMaxRanges
    // calculations.
    // These requirements allow the predicate extract library to remove
    // the residual filter in queries with parameters and optimize query execution.
    // Users must verify these requirements externally,
    // as if these parameters are not met, the extract predicate may produce incorrect
    // lambda expressions without a residual filter, leading to incorrect results.
    TMaybe<size_t> ExternalParameterMaxSize;
};

class IPredicateRangeExtractor {
public:
    using TPtr = THolder<IPredicateRangeExtractor>;

    virtual bool Prepare(const TExprNode::TPtr& filterLambda, const TTypeAnnotationNode& rowType,
        THashSet<TString>& possibleIndexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) = 0;

    struct TBuildResult {
        TExprNode::TPtr ComputeNode;
        TExprNode::TPtr PrunedLambda;
        size_t UsedPrefixLen = 0;
        size_t PointPrefixLen = 0;
        TMaybe<size_t> ExpectedMaxRanges;
        size_t ExternalParameterMaxSizesLookups = 0;

        struct TLiteralRange {
            struct TLiteralRangeBound {
                bool Inclusive = false;
                TVector<TExprNode::TPtr> Columns;
            };

            TLiteralRangeBound Left;
            TLiteralRangeBound Right;
        };

        TMaybe<TLiteralRange> LiteralRange;
    };

    virtual TBuildResult BuildComputeNode(const TVector<TString>& indexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) const = 0;

    virtual ~IPredicateRangeExtractor() = default;
};

IPredicateRangeExtractor::TPtr MakePredicateRangeExtractor(const TPredicateExtractorSettings& settings = {});


TExprNode::TPtr BuildPointsList(const IPredicateRangeExtractor::TBuildResult&, TConstArrayRef<TString> keyColumns, NYql::TExprContext& expCtx);

}
