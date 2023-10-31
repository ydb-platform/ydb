#include "yql_dq_optimization_impl.h"

namespace NYql {

TExprNode::TPtr TDqOptimizationBase::RewriteRead(const TExprNode::TPtr& reader, TExprContext& /*ctx*/) {
    return reader;
}

TExprNode::TPtr TDqOptimizationBase::ApplyExtractMembers(const TExprNode::TPtr& reader, const TExprNode::TPtr& /*members*/, TExprContext& /*ctx*/) {
    return reader;
}

TExprNode::TPtr TDqOptimizationBase::ApplyTakeOrSkip(const TExprNode::TPtr& reader, const TExprNode::TPtr& /*countBase*/, TExprContext& /*ctx*/) {
    return reader;
}

TExprNode::TPtr TDqOptimizationBase::ApplyUnordered(const TExprNode::TPtr& reader, TExprContext& /*ctx*/) {
    return reader;
}

TExprNode::TListType TDqOptimizationBase::ApplyExtend(const TExprNode::TListType& listOfReader, bool /*ordered*/, TExprContext& /*ctx*/) {
    return listOfReader;
}

} // namespace NYql
