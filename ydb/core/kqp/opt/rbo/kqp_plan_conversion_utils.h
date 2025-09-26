#pragma once

#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TIOperatorSharedPtrHash
{
    size_t operator()(const std::shared_ptr<IOperator>& p) const
    {
        return p ? THash<int64_t>{}((int64_t)p.get()) : 0; 
    }
};

class PlanConverter {
    public:
    TOpRoot ConvertRoot(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ExprNodeToOperator(TExprNode::TPtr node);

    std::shared_ptr<IOperator> ConvertTKqpOpMap(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpFilter(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpJoin(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpLimit(TExprNode::TPtr node);
    std::shared_ptr<IOperator> ConvertTKqpOpProject(TExprNode::TPtr node);

    THashMap<TExprNode*, std::shared_ptr<IOperator>> Converted;
};

class ExprNodeRebuilder {
    public:
    ExprNodeRebuilder(TExprContext &ctx, TPositionHandle pos) : Ctx(ctx), Pos(pos) {}

    void RebuildExprNodes(TOpRoot & root);
    void RebuildExprNode(std::shared_ptr<IOperator> op);

    TExprNode::TPtr RebuildEmptySource();
    TExprNode::TPtr RebuildMap(std::shared_ptr<IOperator> op);
    TExprNode::TPtr RebuildProject(std::shared_ptr<IOperator> op);
    TExprNode::TPtr RebuildFilter(std::shared_ptr<IOperator> op);
    TExprNode::TPtr RebuildJoin(std::shared_ptr<IOperator> op);
    TExprNode::TPtr RebuildLimit(std::shared_ptr<IOperator> op);

    TExprContext & Ctx;
    TPositionHandle Pos;
    THashMap<std::shared_ptr<IOperator>, TExprNode::TPtr, TIOperatorSharedPtrHash> RebuiltNodes;
};

}
}