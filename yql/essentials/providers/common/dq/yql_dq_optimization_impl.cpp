#include "yql_dq_optimization_impl.h"

namespace NYql {

TExprNode::TPtr TDqOptimizationBase::RewriteRead(const TExprNode::TPtr& read, TExprContext& /*ctx*/) {
    return read;
}

TExprNode::TPtr TDqOptimizationBase::RewriteLookupRead(const TExprNode::TPtr& /*read*/, TExprContext& /*ctx*/) {
    return {};
}

TExprNode::TPtr TDqOptimizationBase::ApplyExtractMembers(const TExprNode::TPtr& read, const TExprNode::TPtr& /*members*/, TExprContext& /*ctx*/) {
    return read;
}

TExprNode::TPtr TDqOptimizationBase::ApplyTakeOrSkip(const TExprNode::TPtr& read, const TExprNode::TPtr& /*countBase*/, TExprContext& /*ctx*/) {
    return read;
}

TExprNode::TPtr TDqOptimizationBase::ApplyUnordered(const TExprNode::TPtr& read, TExprContext& /*ctx*/) {
    return read;
}

TExprNode::TListType TDqOptimizationBase::ApplyExtend(const TExprNode::TListType& reads, bool /*ordered*/, TExprContext& /*ctx*/) {
    return reads;
}

} // namespace NYql
