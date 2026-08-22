#pragma once

#include <utility>

#include "extract_predicate.h"

namespace NYql::NDetail {

class TPredicateRangeExtractor: public IPredicateRangeExtractor {
public:
    explicit TPredicateRangeExtractor(TPredicateExtractorSettings settings = {})
        : Settings_(std::move(settings))
    {
    }

    bool Prepare(const TExprNode::TPtr& filterLambdaNode, const TTypeAnnotationNode& rowType,
                 THashSet<TString>& possibleIndexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) final;

    TExprNode::TPtr GetPreparedRange() const {
        return Range_;
    }

    TBuildResult BuildComputeNode(const TVector<TString>& indexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) const final;

private:
    const TPredicateExtractorSettings Settings_;
    TExprNode::TPtr FilterLambda_;
    const TStructExprType* RowType_ = nullptr;
    TExprNode::TPtr Range_;
};

} // namespace NYql::NDetail
