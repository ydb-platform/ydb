#include "yql_opt_rewrite_io.h"
#include "yql_expr_optimize.h"

namespace NYql {

IGraphTransformer::TStatus RewriteIO(const TExprNode::TPtr& input, TExprNode::TPtr& output, const TTypeAnnotationContext& types, TExprContext& ctx) {
    if (ctx.Step.IsDone(TExprStep::RewriteIO)) {
        return IGraphTransformer::TStatus::Ok;
    }

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    auto ret = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        YQL_ENSURE(node->Type() == TExprNode::Callable);
        if (node->Content() == LeftName || node->Content() == RightName) {
            auto child = node->Child(0);
            if (child->IsCallable(ReadName)) {
                auto dataSourceName = child->Child(1)->Child(0)->Content();
                auto datasource = types.DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(datasource);
                return (*datasource)->RewriteIO(node, ctx);
            }
        } else if (node->IsCallable(WriteName)) {
            auto dataSinkName = node->Child(1)->Child(0)->Content();
            auto datasink = types.DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(datasink);
            return (*datasink)->RewriteIO(node, ctx);
        }

        return node;
    }, ctx, settings);

    if (ret.Level == IGraphTransformer::TStatus::Error) {
        return ret;
    }

    if (
        !ctx.Step.IsDone(TExprStep::DiscoveryIO) ||
        !ctx.Step.IsDone(TExprStep::ExpandApplyForLambdas) ||
        !ctx.Step.IsDone(TExprStep::ExprEval)
    ) {
        return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
    }

    for (const auto& ds : types.DataSinks)
        ds->PostRewriteIO();
    for (const auto& ds : types.DataSources)
        ds->PostRewriteIO();

    ctx.Step.Done(TExprStep::RewriteIO);
    return IGraphTransformer::TStatus::Ok;
}

}
