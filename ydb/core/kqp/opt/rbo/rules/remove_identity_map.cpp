#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

// Remove extra maps that arrise during translation
// Identity map can be removed when it only renames every visible column to itself.

TIntrusivePtr<IOperator> TRemoveIdenityMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);

    if (map->GetOutputIUs() != map->GetInput()->GetOutputIUs()) {
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
