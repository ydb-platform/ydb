#pragma once

#include "extract_predicate.h"

namespace NYql::NDetail {

class TPredicateRangeExtractor : public IPredicateRangeExtractor {
public:
    explicit TPredicateRangeExtractor(const TPredicateExtractorSettings& settings = {})
        : Settings_(settings)
    {}

    bool Prepare(const TExprNode::TPtr& filterLambdaNode, const TTypeAnnotationNode& rowType,
        THashSet<TString>& possibleIndexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) override final;

    TExprNode::TPtr GetPreparedRange() const {
        return Range_;
    }

    TBuildResult BuildComputeNode(const TVector<TString>& indexKeys, TExprContext& ctx, TTypeAnnotationContext& typesCtx) const override final;
private:
    const TPredicateExtractorSettings Settings_;
    TExprNode::TPtr FilterLambda_;
    const TStructExprType* RowType_ = nullptr;
    TExprNode::TPtr Range_;
};

}
