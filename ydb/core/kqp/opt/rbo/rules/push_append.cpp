#include "kqp_rules_include.h"

#include <algorithm>

namespace NKikimr {
namespace NKqp {

namespace {

enum class EPushTarget {
    Top,
    Left,
    Right
};

bool DependenciesAvailable(const TMapElement& mapElement, const TVector<TInfoUnit>& outputIUs) {
    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    return IUSetDiff(usedIUs, outputIUs).empty();
}

TInfoUnitSet GetRenameSources(const TIntrusivePtr<TOpMap>& map) {
    TInfoUnitSet result;
    for (const auto& mapElement : map->MapElements) {
        if (mapElement.IsRename()) {
            result.insert(mapElement.GetRename());
        }
    }
    return result;
}

const TMapElement* FindProducedMapElement(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& iu) {
    const auto it = std::find_if(map->MapElements.begin(), map->MapElements.end(), [&iu](const TMapElement& element) {
        return element.GetElementName() == iu;
    });
    return it == map->MapElements.end() ? nullptr : &*it;
}

bool CanMoveAppendElement(const TMapElement& mapElement, const TInfoUnitSet& blockedOutputs) {
    return !mapElement.IsRename() && !blockedOutputs.contains(mapElement.GetElementName());
}

bool IsLeftPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross" || joinKind == "Left" || joinKind == "LeftOnly" || joinKind == "LeftSemi";
}

bool IsRightPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross" || joinKind == "Right" || joinKind == "RightOnly" || joinKind == "RightSemi";
}

EPushTarget SelectJoinPushTarget(
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& leftOutput,
    const TVector<TInfoUnit>& rightOutput,
    const TString& joinKind)
{
    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    const bool leftAvailable = IUSetDiff(usedIUs, leftOutput).empty();
    const bool rightAvailable = IUSetDiff(usedIUs, rightOutput).empty();
    const bool leftPreserved = IsLeftPreserved(joinKind);
    const bool rightPreserved = IsRightPreserved(joinKind);

    // Side-independent expressions must still be pushed to a side that exists
    // for every output row. For Full join, neither side satisfies that.
    if (usedIUs.empty()) {
        if (leftPreserved) {
            return EPushTarget::Left;
        }
        if (rightPreserved) {
            return EPushTarget::Right;
        }
        return EPushTarget::Top;
    }

    if (leftAvailable && leftPreserved) {
        return EPushTarget::Left;
    }
    if (rightAvailable && rightPreserved) {
        return EPushTarget::Right;
    }
    return EPushTarget::Top;
}

bool TryPushElementToMap(
    const TIntrusivePtr<TOpMap>& bottomMap,
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& bottomInputIUs,
    const TPlanProps& props)
{
    if (!DependenciesAvailable(mapElement, bottomInputIUs)) {
        return false;
    }

    bottomMap->MapElements.push_back(mapElement);
    if (!CanExposeOutput(bottomMap, bottomMap->GetOutputIUs(), props)) {
        bottomMap->MapElements.pop_back();
        return false;
    }

    return true;
}

bool TryComposeAliasAndPushToMap(
    const TIntrusivePtr<TOpMap>& bottomMap,
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& bottomInputIUs,
    const TPlanProps& props)
{
    if (!mapElement.IsColumnAccess()) {
        return false;
    }

    const auto* bottomElement = FindProducedMapElement(bottomMap, mapElement.GetColumnAccess());
    if (!bottomElement) {
        return false;
    }

    TMapElement composedElement = mapElement;
    composedElement.SetExpression(bottomElement->GetExpression());
    if (!DependenciesAvailable(composedElement, bottomInputIUs)) {
        return false;
    }

    bottomMap->MapElements.push_back(composedElement);
    if (!CanExposeOutput(bottomMap, bottomMap->GetOutputIUs(), props)) {
        bottomMap->MapElements.pop_back();
        return false;
    }

    return true;
}

TIntrusivePtr<IOperator> PushAppendElementsIntoMap(const TIntrusivePtr<TOpMap>& map, const TPlanProps& props) {
    auto bottomMap = CastOperator<TOpMap>(map->GetInput());
    const auto bottomInputIUs = bottomMap->GetInput()->GetOutputIUs();
    const auto blockedOutputs = GetRenameSources(map);
    auto originalBottomElements = bottomMap->MapElements;

    TVector<TMapElement> topElements;
    bool pushed = false;

    for (const auto& mapElement : map->MapElements) {
        if (!CanMoveAppendElement(mapElement, blockedOutputs)) {
            topElements.push_back(mapElement);
            continue;
        }

        if (TryPushElementToMap(bottomMap, mapElement, bottomInputIUs, props) ||
            TryComposeAliasAndPushToMap(bottomMap, mapElement, bottomInputIUs, props)) {
            pushed = true;
        } else {
            topElements.push_back(mapElement);
        }
    }

    if (!pushed) {
        return map;
    }

    if (topElements.empty()) {
        if (!CanReplaceInParents(map, bottomMap, props)) {
            bottomMap->MapElements = std::move(originalBottomElements);
            return map;
        }
        return bottomMap;
    }

    return MakeIntrusive<TOpMap>(bottomMap, map->Pos, topElements, map->Ordered);
}

TIntrusivePtr<IOperator> PushAppendElementsThroughJoin(const TIntrusivePtr<TOpMap>& map, const TPlanProps& props) {
    auto join = CastOperator<TOpJoin>(map->GetInput());
    const auto blockedOutputs = GetRenameSources(map);
    const auto originalLeftInput = join->GetLeftInput();
    const auto originalRightInput = join->GetRightInput();

    // Make sure the join and its inputs are single consumer.
    // FIXME: join inputs don't have to be single consumer, but this used to break due to multiple consumer problem.
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return map;
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<std::pair<TMapElement, EPushTarget>> classifiedElements;
    const auto leftOutput = join->GetLeftInput()->GetOutputIUs();
    const auto rightOutput = join->GetRightInput()->GetOutputIUs();

    for (const auto& mapElement : map->MapElements) {
        if (!CanMoveAppendElement(mapElement, blockedOutputs)) {
            classifiedElements.emplace_back(mapElement, EPushTarget::Top);
        } else {
            const auto target = SelectJoinPushTarget(mapElement, leftOutput, rightOutput, join->JoinKind);
            if (target == EPushTarget::Left) {
                leftMapElements.push_back(mapElement);
            } else if (target == EPushTarget::Right) {
                rightMapElements.push_back(mapElement);
            }
            classifiedElements.emplace_back(mapElement, target);
        }
    }

    if (leftMapElements.empty() && rightMapElements.empty()) {
        return map;
    }

    TIntrusivePtr<TOpMap> leftMap;
    bool pushLeft = !leftMapElements.empty();
    if (!leftMapElements.empty()) {
        leftMap = MakeIntrusive<TOpMap>(join->GetLeftInput(), map->Pos, leftMapElements);
        if (HasOutputConflicts(leftMap->GetOutputIUs())) {
            pushLeft = false;
            leftMap = nullptr;
        }
    }

    TIntrusivePtr<TOpMap> rightMap;
    bool pushRight = !rightMapElements.empty();
    if (!rightMapElements.empty()) {
        rightMap = MakeIntrusive<TOpMap>(join->GetRightInput(), map->Pos, rightMapElements);
        if (HasOutputConflicts(rightMap->GetOutputIUs())) {
            pushRight = false;
            rightMap = nullptr;
        }
    }

    if (!pushLeft && !pushRight) {
        return map;
    }

    if (leftMap) {
        join->SetLeftInput(leftMap);
    }

    if (rightMap) {
        join->SetRightInput(rightMap);
    }

    if (HasOutputConflicts(join->GetOutputIUs())) {
        join->SetLeftInput(originalLeftInput);
        join->SetRightInput(originalRightInput);
        return map;
    }

    TVector<TMapElement> topMapElements;
    for (const auto& [mapElement, target] : classifiedElements) {
        if (target == EPushTarget::Top ||
            (target == EPushTarget::Left && !pushLeft) ||
            (target == EPushTarget::Right && !pushRight)) {
            topMapElements.push_back(mapElement);
        }
    }

    if (topMapElements.empty()) {
        if (!CanReplaceInParents(map, join, props)) {
            join->SetLeftInput(originalLeftInput);
            join->SetRightInput(originalRightInput);
            return map;
        }
        return join;
    }

    return MakeIntrusive<TOpMap>(join, map->Pos, topMapElements, map->Ordered);
}

} // anonymous namespace

// Push append-only map elements closer to sources. If only some elements can move, leave the rest above.
// Semantic renames are a barrier: they change visible bindings, so this rule does not move them.

TIntrusivePtr<IOperator> TPushAppendRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);

    if (map->GetInput()->Kind == EOperator::Map && map->GetInput()->IsSingleConsumer()) {
        return PushAppendElementsIntoMap(map, props);
    }

    if (map->GetInput()->Kind == EOperator::Join) {
        return PushAppendElementsThroughJoin(map, props);
    }

    return input;
}
}
}
