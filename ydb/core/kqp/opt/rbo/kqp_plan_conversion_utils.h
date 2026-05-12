#pragma once

#include "kqp_operator.h"

/**
 * Convert a plan from ExprNode operators into RBO operators and back
 */
namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TIOperatorSharedPtrHash {
    size_t operator()(const TIntrusivePtr<IOperator> &p) const { return p ? THash<int64_t>{}((int64_t)p.get()) : 0; }
};

class PlanConverter {
  public:
    PlanConverter(TTypeAnnotationContext &typeCtx, TExprContext &ctx) : TypeCtx(typeCtx), Ctx(ctx) {}

    // Convert KqpOpRoot to OpRoot.
    TIntrusivePtr<TOpRoot> ConvertRoot(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ExprNodeToOperator(TExprNode::TPtr node);

    TIntrusivePtr<IOperator> ConvertTKqpOpMap(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpFilter(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpJoin(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpLimit(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpProject(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpUnionAll(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpSort(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpOpAggregate(TExprNode::TPtr node);
    TIntrusivePtr<IOperator> ConvertTKqpInfuseDependents(TExprNode::TPtr node);

    TExprNode::TPtr RemoveSubplans(TExprNode::TPtr lambda);

    TTypeAnnotationContext &TypeCtx;
    TExprContext &Ctx;
    THashMap<TExprNode*, TIntrusivePtr<IOperator>> Converted;
    TPlanProps PlanProps;

};

} // namespace NKqp
} // namespace NKikimr