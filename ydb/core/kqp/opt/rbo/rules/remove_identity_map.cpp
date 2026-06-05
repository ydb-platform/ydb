#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool IsIdentityRename(const TMapElement& mapElement) {
    return mapElement.IsRename() && mapElement.GetRename() == mapElement.GetElementName();
}

bool CanRemoveWholeMap(const TIntrusivePtr<TOpMap>& map, const TPlanProps& props) {
    return map->GetOutputIUs() == map->GetInput()->GetOutputIUs() &&
        CanReplaceInParents(map, map->GetInput(), props);
}

} // anonymous namespace

// Remove extra maps that arrise during translation.
// Identity renames carry no semantic rename and should not block map rewrites.

TIntrusivePtr<IOperator> TRemoveIdenityMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);
    if (map->MapElements.empty()) {
        return CanRemoveWholeMap(map, props) ? map->GetInput() : input;
    }

    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());
    bool removed = false;
    for (const auto& mapElement : map->MapElements) {
        if (IsIdentityRename(mapElement)) {
            removed = true;
            continue;
        }
        newElements.push_back(mapElement);
    }

    if (!removed) {
        return input;
    }

    if (newElements.empty()) {
        return CanRemoveWholeMap(map, props) ? map->GetInput() : input;
    }

    return MakeIntrusive<TOpMap>(map->GetInput(), map->Pos, map->Props, newElements, map->IsOrdered());
}

}
}
