#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool IsIdentityRename(const TMapElement& mapElement) {
    return mapElement.IsRename() && mapElement.GetRename() == mapElement.GetElementName();
}

bool IsIdentityAppend(const TMapElement& mapElement) {
    return !mapElement.IsRename() &&
        mapElement.IsColumnAccess() &&
        mapElement.GetColumnAccess() == mapElement.GetElementName();
}

bool IsIdentityElement(const TMapElement& mapElement) {
    return IsIdentityRename(mapElement) || IsIdentityAppend(mapElement);
}

TInfoUnitSet GetInputIUs(const TIntrusivePtr<TOpMap>& map) {
    TInfoUnitSet result;
    for (const auto& iu : map->GetInput()->GetOutputIUs()) {
        result.insert(iu);
    }
    return result;
}

} // anonymous namespace

// Remove extra maps that arrise during translation.
// Identity renames carry no semantic rename and should not block map rewrites.

TIntrusivePtr<IOperator> TRemoveIdenityMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);
    if (map->MapElements.empty()) {
        return map->GetInput();
    }

    const auto inputIUs = GetInputIUs(map);
    TInfoUnitSet identitySources;
    for (const auto& mapElement : map->MapElements) {
        if (IsIdentityElement(mapElement) && inputIUs.contains(mapElement.GetElementName())) {
            identitySources.insert(mapElement.GetElementName());
        }
    }

    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());
    TInfoUnitSet remainingRenameSources;
    bool changed = false;
    for (auto mapElement : map->MapElements) {
        if (mapElement.IsRename() &&
            !IsIdentityRename(mapElement) &&
            identitySources.contains(mapElement.GetRename())) {
            mapElement.SetIsRename(false);
            changed = true;
        }

        if (mapElement.IsRename() && !IsIdentityRename(mapElement)) {
            remainingRenameSources.insert(mapElement.GetRename());
        }

        newElements.push_back(mapElement);
    }

    TVector<TMapElement> finalElements;
    finalElements.reserve(newElements.size());
    for (const auto& mapElement : newElements) {
        if (IsIdentityElement(mapElement) &&
            inputIUs.contains(mapElement.GetElementName()) &&
            !remainingRenameSources.contains(mapElement.GetElementName())) {
            changed = true;
            continue;
        }

        finalElements.push_back(mapElement);
    }

    if (!changed) {
        return input;
    }

    if (finalElements.empty()) {
        return map->GetInput();
    }

    return MakeIntrusive<TOpMap>(map->GetInput(), map->Pos, map->Props, finalElements, map->IsOrdered());
}

}
}
