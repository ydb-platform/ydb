#include "kqp_rbo.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

bool TSimplifiedRule::TestAndApply(std::shared_ptr<IOperator>& input, 
    TExprContext& ctx,
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    auto output = SimpleTestAndApply(input, ctx, kqpCtx, typeCtx, config, props);
    if (input != output) {
        input = output;
        return true;
    }
    else {
        return false;
    }
}

void TRuleBasedStage::RunStage(TRuleBasedOptimizer* optimizer, TOpRoot & root, TExprContext& ctx) {

    bool fired = true;

    int nMatches = 0;

    while (fired && nMatches < 1000) {
        fired = false;

        for (auto iter : root ) {
            for (auto rule : Rules) {
                auto op = iter.Current;

                if (rule->TestAndApply(op, ctx, optimizer->KqpCtx, optimizer->TypeCtx, optimizer->Config, root.PlanProps)) {
                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                    if (iter.Parent) {
                        iter.Parent->Children[iter.ChildIndex] = op;
                    }
                    else {
                        root.Children[0] = op;
                    }

                    fired = true;

                    if (rule->RequiresParentRecompute) {
                        root.ComputeParents();
                    }

                    if (RequiresRebuild) {
                        ExprNodeRebuilder(ctx, root.Node->Pos()).RebuildExprNodes(root);
                        YQL_CLOG(TRACE, CoreDq) << "After rule " << rule->RuleName << ":\n" << KqpExprToPrettyString(NYql::NNodes::TExprBase(root.Node), ctx);
                    }

                    nMatches ++;
                    break;
                }
            }

            if (fired) {
                break;
            }
        }
    }

    Y_ENSURE(nMatches < 100);
}

TExprNode::TPtr TRuleBasedOptimizer::Optimize(TOpRoot & root,  TExprContext& ctx) {

    ExprNodeRebuilder(ctx, root.Node->Pos()).RebuildExprNodes(root);
    YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << KqpExprToPrettyString(NYql::NNodes::TExprBase(root.Node), ctx);
    YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << root.PlanToString();

    for (size_t idx=0; idx < Stages.size(); idx ++ ) {
        YQL_CLOG(TRACE, CoreDq) << "Running stage: " << idx;
        auto stage = Stages[idx];
        stage->RunStage(this, root, ctx);
        ExprNodeRebuilder(ctx, root.Node->Pos()).RebuildExprNodes(root);
        YQL_CLOG(TRACE, CoreDq) << "After stage:\n" << KqpExprToPrettyString(NYql::NNodes::TExprBase(root.Node), ctx);
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    return ConvertToPhysical(root, ctx, TypeCtx, TypeAnnTransformer, PeepholeTransformer, Config);
}

}
}