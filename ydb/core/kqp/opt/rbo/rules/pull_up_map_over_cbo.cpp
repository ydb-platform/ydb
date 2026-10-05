#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Pull up map operator over CBO tree
// We match the parent of the map

TIntrusivePtr<IOperator> TPullUpMapOverCBORule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);

    for (size_t index = 0; index < input->GetChildCount(); ++index) {
        auto child = input->GetChild(index);
        if (child->Kind == EOperator::Map && CastOperator<TOpMap>(child)->GetInput()->Kind == EOperator::CBOTree) {
            auto map = CastOperator<TOpMap>(child);

            YQL_CLOG(TRACE, CoreDq) << "Trying to pull up map";

            // We can always pull up a map above the join, unless we try to pull up from a right side of a non-inner join
            // But we need to check that the join doesn't depend on the map
            if (input->Kind == EOperator::Join) {
                auto join = CastOperator<TOpJoin>(input);
                if (join->JoinKind != "Inner" && join->GetLeftInput() != map) {
                    continue;
                }
                const auto& joinKeys = join->GetLeftInput() == map ? join->GetLHSKeys() : join->GetRHSKeys();
                if (!joinKeys.IsSubsetOf(map->GetInput()->GetOutputIUs())) {
                    continue;
                }
            }

            // We also pull up the map above filters, but only if the filter doesn't depend on map output
            else if (input->Kind == EOperator::Filter) {
                auto filter = CastOperator<TOpFilter>(input);
                if (!filter->GetUsedIUs(props).IsSubsetOf(map->GetInput()->GetOutputIUs())) {
                    continue;
                }
            }

            // We don't pull up in other cases
            else {
                continue;
            }

            // Perform the actual pull-up
            input->SetChild(index, map->GetInput());
            map->SetInput(input);
            return map;
        }
        else {
            continue;
        }
    }

    return input;
}

}
}
