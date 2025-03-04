#include "kqp_new_rbo.h"

namespace NKikimr {
namespace NKqp {

void TRuleBasedOptimizer::Optimize(TOpRoot & root,  TExprContext& ctx) {
    for (auto & ruleSet : Stages) {
        bool fired = true;

        while (fired) {
            fired = false;

            TVector<std::shared_ptr<IOperator>*> desc = root.DescendantsDFS();
            for (auto op : desc) {
                for (auto rule : ruleSet) {
                    if (rule->TestAndApply(*op, ctx, KqpCtx, TypeCtx, Config)) {
                        auto newRoot = std::static_pointer_cast<TOpRoot>(root.Rebuild(ctx));
                        root.Node = newRoot->Node;
                        root.Children[0] = newRoot->Children[0];
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