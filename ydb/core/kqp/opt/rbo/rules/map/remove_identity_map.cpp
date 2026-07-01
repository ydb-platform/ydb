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

    TInfoUnitSet identityAppendSources;
    for (const auto& mapElement : map->MapElements) {
        if (IsIdentityAppend(mapElement)) {
            identityAppendSources.insert(mapElement.GetElementName());
        }
    }

    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());
    TInfoUnitSet remainingRenameSources;
    TInfoUnitSet convertedRenameSources;
    bool changed = false;
    for (auto mapElement : map->MapElements) {
        if (IsIdentityRename(mapElement)) {
            changed = true;
            continue;
        }

        if (mapElement.IsRename() &&
            identityAppendSources.contains(mapElement.GetRename())) {
            convertedRenameSources.insert(mapElement.GetRename());
            mapElement.SetIsRename(false);
            changed = true;
        }

        if (mapElement.IsRename()) {
            remainingRenameSources.insert(mapElement.GetRename());
        }

        newElements.push_back(mapElement);
    }

    TVector<TMapElement> finalElements;
    finalElements.reserve(newElements.size());
    for (const auto& mapElement : newElements) {
        if (IsIdentityAppend(mapElement) &&
            convertedRenameSources.contains(mapElement.GetElementName()) &&
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
