#include "kqp_rbo.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

bool ISimplifiedRule::TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    auto output = SimpleTestAndApply(input, ctx, props);
    if (input != output) {
        input = output;
        return true;
    } else {
        return false;
    }
}

TRuleBasedStage::TRuleBasedStage(TVector<std::shared_ptr<IRule>> rules) : Rules(rules) {
    for (auto & r : Rules) {
        Props.RequireCosts |= r->Props.RequireCosts;
        Props.RequireParents |= r->Props.RequireParents;
        Props.RequireTableMeta |= r->Props.RequireTableMeta;
        Props.RequireTypes |= r->Props.RequireTypes;
    }
}

void ComputeRequiredProps(TOpRoot &root, TRuleProperties &props, TRBOContext &ctx) {
    if (props.RequireParents) {
        root.ComputeParents();
    }
    if (props.RequireTypes) {
        if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
            Y_ENSURE(false, "RBO type annotation failed");
        }
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
void TRuleBasedStage::RunStage(TOpRoot &root, TRBOContext &ctx) {
    bool fired = true;
    int nMatches = 0;

    while (fired && nMatches < 1000) {
        fired = false;

        for (auto iter : root) {
            for (auto rule : Rules) {
                auto op = iter.Current;

                if (rule->TestAndApply(op, ctx, root.PlanProps)) {
                    fired = true;

                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                    if (iter.Parent) {
                        iter.Parent->Children[iter.ChildIndex] = op;
                    } else {
                        root.Children[0] = op;
                    }

                    ComputeRequiredProps(root, Props, ctx);

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

    auto context = TRBOContext(KqpCtx,ctx,TypeCtx, RBOTypeAnnTransformer, FuncRegistry);

    for (size_t idx = 0; idx < Stages.size(); idx++) {
        YQL_CLOG(TRACE, CoreDq) << "Running stage: " << idx;
        auto stage = Stages[idx];
        ComputeRequiredProps(root, stage->Props, context);
        stage->RunStage(root, context);
        YQL_CLOG(TRACE, CoreDq) << "After stage:\n" << root.PlanToString(ctx);
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    TRuleProperties convertStageProps;
    convertStageProps.RequireParents = true;
    convertStageProps.RequireTypes = true;
    ComputeRequiredProps(root, convertStageProps, context);

    return ConvertToPhysical(root, context, TypeAnnTransformer, PeepholeTransformer);
}
} // namespace NKqp
} // namespace NKikimr