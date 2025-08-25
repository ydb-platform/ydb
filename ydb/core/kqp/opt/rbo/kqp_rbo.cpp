#include "kqp_rbo.h"
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

TExprNode::TPtr TRuleBasedOptimizer::Optimize(TOpRoot & root,  TExprContext& ctx) {
    for (size_t idx=0; idx < Stages.size(); idx ++ ) {
        YQL_CLOG(TRACE, CoreDq) << "Running ruleset: " << idx;
        auto & stage = Stages[idx];

        bool fired = true;

        int nMatches = 0;

        while (fired && nMatches < 1000) {
            fired = false;

            for (auto iter : root ) {
                for (auto rule : stage.Rules) {
                    auto op = iter.Current;

                    if (rule->TestAndApply(op, ctx, KqpCtx, TypeCtx, Config, root.PlanProps)) {
                        YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                        if (iter.Parent) {
                            iter.Parent->Children[iter.ChildIndex] = op;
                        }
                        else {
                            root.Children[0] = op;
                        }

                        fired = true;

                        if (stage.RequiresRebuild) {
                            auto newRoot = std::static_pointer_cast<TOpRoot>(root.Rebuild(ctx));
                            YQL_CLOG(TRACE, CoreDq) << "Rebuilt tree";
                            root.Node = newRoot->Node;
                            root.Children[0] = newRoot->Children[0];
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
    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    return ConvertToPhysical(root, ctx, TypeCtx, TypeAnnTransformer, PeepholeTransformer, Config);
}

}
}