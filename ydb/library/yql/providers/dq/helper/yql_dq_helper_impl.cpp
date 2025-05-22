#include "yql_dq_helper_impl.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>


namespace NYql {

using namespace NNodes;

class TDqHelperImpl: public IDqHelper {
public:
    bool IsSingleConsumerConnection(const TExprNode::TPtr& node, const TParentsMap& parentsMap) final {
        YQL_ENSURE(TDqCnUnionAll::Match(node.Get()));
        return NDq::IsSingleConsumerConnection(TDqCnUnionAll(node), parentsMap);
    }

    TExprNode::TPtr PushLambdaAndCreateCnResult(const TExprNode::TPtr& conn, const TExprNode::TPtr& lambda, TPositionHandle pos,
        TExprContext& ctx, IOptimizationContext& optCtx) final
    {
        YQL_ENSURE(TDqCnUnionAll::Match(conn.Get()));
        YQL_ENSURE(lambda->IsLambda());

        auto dqUnion = TDqCnUnionAll(conn);
        TMaybeNode<TDqConnection> result;
        if (NDq::GetStageOutputsCount(dqUnion.Output().Stage()) > 1) {
            result = Build<TDqCnUnionAll>(ctx, pos)
                .Output()
                    .Stage<TDqStage>()
                        .Inputs()
                            .Add(dqUnion)
                        .Build()
                        .Program(lambda)
                        .Settings(NDq::TDqStageSettings().BuildNode(ctx, pos))
                    .Build()
                    .Index().Build("0")
                .Build()
                .Done().Ptr();
        } else {
            result = NDq::DqPushLambdaToStageUnionAll(dqUnion, TCoLambda(lambda), {}, ctx, optCtx);
            if (!result) {
                return {};
            }
        }

        return Build<TDqCnResult>(ctx, pos)
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(result.Cast())
                    .Build()
                    .Program()
                        .Args({"row"})
                        .Body("row")
                    .Build()
                    .Settings(NDq::TDqStageSettings().BuildNode(ctx, pos))
                .Build()
                .Index().Build("0")
            .Build()
            .ColumnHints() // TODO: set column hints
            .Build()
            .Done().Ptr();
    }

    TExprNode::TPtr CreateDqStageSettings(bool singleTask, TExprContext& ctx, TPositionHandle pos) final {
        NDq::TDqStageSettings settings;
        settings.PartitionMode = singleTask ? NDq::TDqStageSettings::EPartitionMode::Single : NDq::TDqStageSettings::EPartitionMode::Default;
        return settings.BuildNode(ctx, pos).Ptr();
    }

    TExprNode::TListType RemoveVariadicDqStageSettings(const TExprNode& settings) final {
        TExprNode::TListType res;

        for (auto n: settings.Children()) {
            if (n->Type() == TExprNode::List
                && n->ChildrenSize() > 0
                && n->Child(0)->Content() == NDq::TDqStageSettings::LogicalIdSettingName) {
                continue;
            }
            res.push_back(n);
        }
        return res;
    }
};


IDqHelper::TPtr MakeDqHelper() {
    return std::make_shared<TDqHelperImpl>();
}

} // namespace NYql
