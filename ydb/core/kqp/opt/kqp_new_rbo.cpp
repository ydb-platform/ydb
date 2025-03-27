#include "kqp_new_rbo.h"
#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

void TRuleBasedOptimizer::Optimize(TOpRoot & root,  TExprContext& ctx) {
    for (auto & ruleSet : Stages) {
        bool fired = true;

        while (fired) {
            fired = false;

            for (auto iter : root ) {
                for (auto rule : ruleSet) {
                    auto op = iter.Current;
                    auto newOp = rule->TestAndApply(op, ctx, KqpCtx, TypeCtx, Config);

                    if (op != newOp) {
                        if (iter.Parent) {
                            iter.Parent->Children[iter.ChildIndex] = newOp;
                        }
                        else {
                            root.Children[0] = newOp;
                        }

                        auto newRoot = std::static_pointer_cast<TOpRoot>(root.Rebuild(ctx));
                        root.Node = newRoot->Node;
                        root.Children[0] = newRoot->Children[0];
                        fired = true;
                        YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;
                        break;
                    }
                }

                if (fired) {
                    break;
                }
            }
        }
    }
}

}
}