#include "kqp_rbo.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

bool TSimplifiedRule::TestAndApply(std::shared_ptr<IOperator> &input, TExprContext &ctx, const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                   TTypeAnnotationContext &typeCtx, const TKikimrConfiguration::TPtr &config, TPlanProps &props) {

    auto output = SimpleTestAndApply(input, ctx, kqpCtx, typeCtx, config, props);
    if (input != output) {
        input = output;
        return true;
    } else {
        return false;
    }
}
/**
 * Run a rule-based stage
 *
 * Currently we obtain an iterator to the operators, match the rules, and if at least one matched we
 * apply it and start again.
 *
 * TODO: We should have a clear list of properties that are reqiuired by the rules of current stage and
 * ensure they are computed/maintained properly
 *
 * TODO: Add sanity checks that can be tunred on in debug mode to immediately catch transformation problems
 */
void TRuleBasedStage::RunStage(TRuleBasedOptimizer *optimizer, TOpRoot &root, TExprContext &ctx) {
    bool fired = true;
    int nMatches = 0;

    while (fired && nMatches < 1000) {
        fired = false;

        for (auto iter : root) {
            for (auto rule : Rules) {
                auto op = iter.Current;

                if (rule->TestAndApply(op, ctx, optimizer->KqpCtx, optimizer->TypeCtx, optimizer->Config, root.PlanProps)) {
                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                    if (iter.Parent) {
                        iter.Parent->Children[iter.ChildIndex] = op;
                    } else {
                        root.Children[0] = op;
                    }

                    fired = true;

                    if (rule->RequiresParentRecompute) {
                        root.ComputeParents();
                    }

                    nMatches++;
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

TExprNode::TPtr TRuleBasedOptimizer::Optimize(TOpRoot &root, TExprContext &ctx) {
    YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << root.PlanToString(ctx);

    for (size_t idx = 0; idx < Stages.size(); idx++) {
        YQL_CLOG(TRACE, CoreDq) << "Running stage: " << idx;
        auto stage = Stages[idx];
        stage->RunStage(this, root, ctx);
        YQL_CLOG(TRACE, CoreDq) << "After stage:\n" << root.PlanToString(ctx);
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    return ConvertToPhysical(root, ctx, TypeCtx, TypeAnnTransformer, PeepholeTransformer, Config);
}
} // namespace NKqp
} // namespace NKikimr