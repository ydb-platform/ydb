#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {

struct TPredicateExtractorSettings {
    size_t MaxRanges = 10000;
    bool MergeAdjacentPointRanges = true;
    bool HaveNextValueCallable = false;
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
    };

    virtual TBuildResult BuildComputeNode(const TVector<TString>& indexKeys, TExprContext& ctx) const = 0;

    virtual ~IPredicateRangeExtractor() = default;
};

IPredicateRangeExtractor::TPtr MakePredicateRangeExtractor(const TPredicateExtractorSettings& settings = {});

}
