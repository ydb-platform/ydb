#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Remove extra maps that arrise during translation
// Identity map that doesn't project can always be removed
// Identity map that projects maybe a projection operator and can be removed if it doesn't do any extra
// projections

std::shared_ptr<IOperator> TRemoveIdenityMapRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);

    /***
     * If its a project map, check that it output the same number of columns as the operator below
     */

    if (map->Project && map->GetOutputIUs().size() != map->GetInput()->GetOutputIUs().size()) {
        return input;
    }

    for (const auto& mapElement : map->MapElements) {
        if (!mapElement.IsRename()) {
            return input;
        }
        auto fromColumn = mapElement.GetRename();
        if (fromColumn != mapElement.GetElementName()) {
            return input;
        }
    }

    return map->GetInput();
}

}
}