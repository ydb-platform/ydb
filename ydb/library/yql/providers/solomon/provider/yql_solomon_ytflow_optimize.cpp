#include "yql_solomon_ytflow_optimize.h"

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>

#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>

namespace NYql {

using namespace NNodes;

class TSolomonYtflowOptimization : public TEmptyYtflowOptimization {
public:
    TSolomonYtflowOptimization(const TSolomonState::TPtr& state)
        : State_(state.Get())
    {
    }

    TExprNode::TPtr ApplyExtractMembers(
        const TExprNode::TPtr& /*read*/, const TExprNode::TPtr& /*members*/, TExprContext& /*ctx*/
    ) override {
        YQL_ENSURE(false, "Method " << __FUNCTION__ << " is not implemented");
    }

    TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& /*read*/, TExprContext& /*ctx*/) override {
        YQL_ENSURE(false, "Method " << __FUNCTION__ << " is not implemented");
    }

    TExprNode::TPtr TrimWriteContent(const TExprNode::TPtr& write, TExprContext& ctx) override {
        auto maybeWriteToShard = TMaybeNode<TSoWriteToShard>(write);
        if (!maybeWriteToShard) {
            return write;
        }

        auto* listType = maybeWriteToShard.Cast().Input().Ref().GetTypeAnn();
        auto* itemType = listType->Cast<TListExprType>()->GetItemType();

        return Build<TSoWriteToShard>(ctx, write->Pos())
            .InitFrom(maybeWriteToShard.Cast())
            .Input<TYtflowReadStub>()
                .World(ctx.NewWorld(TPositionHandle{}))
                .ItemType(ExpandType(TPositionHandle{}, *itemType, ctx))
                .Build()
            .Done().Ptr();
    }

private:
    TSolomonState* State_;
};

THolder<IYtflowOptimization> CreateSolomonYtflowOptimization(const TSolomonState::TPtr& state) {
    YQL_ENSURE(state);
    return MakeHolder<TSolomonYtflowOptimization>(state);
}

} // namespace NYql
