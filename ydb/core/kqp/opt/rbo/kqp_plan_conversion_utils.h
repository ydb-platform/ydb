#pragma once

#include "kqp_operator.h"

/**
 * Convert a plan from ExprNode operators into RBO operators and back
 */
namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TIOperatorSharedPtrHash {
    size_t operator()(const std::shared_ptr<IOperator> &p) const { return p ? THash<int64_t>{}((int64_t)p.get()) : 0; }
};

class PlanConverter {
  public:
    PlanConverter(TTypeAnnotationContext &typeCtx, TExprContext &ctx) : TypeCtx(typeCtx), Ctx(ctx) {}

    // Convert KqpOpRoot to OpRoot.
    std::unique_ptr<TOpRoot> ConvertRoot(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ExprNodeToOperator(TExprNode::TPtr node);

    std::shared_ptr<IOperator> ConvertTKqpOpMap(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpFilter(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpJoin(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpLimit(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpProject(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpUnionAll(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpSort(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpAggregate(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpInfuseDependents(TExprNode::TPtr node);

    TExprNode::TPtr RemoveSubplans(TExprNode::TPtr lambda);

    TTypeAnnotationContext &TypeCtx;
    TExprContext &Ctx;
    THashMap<TExprNode*, std::shared_ptr<IOperator>> Converted;
    TPlanProps PlanProps;

};

} // namespace NKqp
} // namespace NKikimr