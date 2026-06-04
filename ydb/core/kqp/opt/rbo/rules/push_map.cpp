#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

enum class EPushTarget {
    Top,
    Left,
    Right
};

bool HasDuplicateOutputs(const TIntrusivePtr<IOperator>& op) {
    THashSet<TInfoUnit, TInfoUnit::THashFunction> seen;
    for (const auto& iu : op->GetOutputIUs()) {
        if (seen.contains(iu)) {
            return true;
        }
        seen.insert(iu);
    }
    return false;
}

bool DependenciesAvailable(const TMapElement& mapElement, const TVector<TInfoUnit>& outputIUs) {
    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    return IUSetDiff(usedIUs, outputIUs).empty();
}

bool CanPushMap(const TIntrusivePtr<TOpMap>& map) {
    return !map->HasRenames();
}

TIntrusivePtr<IOperator> SinkMapElementsToMap(const TIntrusivePtr<TOpMap>& map) {
    auto bottomMap = CastOperator<TOpMap>(map->GetInput());
    const auto bottomInputIUs = bottomMap->GetInput()->GetOutputIUs();

    TVector<TMapElement> topElements;
    bool pushed = false;

    for (const auto& mapElement : map->MapElements) {
        if (DependenciesAvailable(mapElement, bottomInputIUs)) {
            bottomMap->MapElements.push_back(mapElement);
            if (HasDuplicateOutputs(bottomMap)) {
                bottomMap->MapElements.pop_back();
                topElements.push_back(mapElement);
            } else {
                pushed = true;
            }
        } else {
            topElements.push_back(mapElement);
        }
    }

    if (!pushed) {
        return map;
    }

    if (topElements.empty()) {
        return bottomMap;
    }

    return MakeIntrusive<TOpMap>(bottomMap, map->Pos, topElements, map->Ordered);
}

TIntrusivePtr<IOperator> PushMapThroughJoin(const TIntrusivePtr<TOpMap>& map) {
    auto join = CastOperator<TOpJoin>(map->GetInput());
    bool canPushRight = join->JoinKind != "Left" && join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";
    bool canPushLeft = join->JoinKind != "Right" && join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";

    // Make sure the join and its inputs are single consumer.
    // FIXME: join inputs don't have to be single consumer, but this used to break due to multiple consumer problem.
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return map;
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<std::pair<TMapElement, EPushTarget>> classifiedElements;

    for (const auto& mapElement : map->MapElements) {
        if (DependenciesAvailable(mapElement, join->GetLeftInput()->GetOutputIUs()) && canPushLeft) {
            leftMapElements.push_back(mapElement);
            classifiedElements.emplace_back(mapElement, EPushTarget::Left);
        } else if (DependenciesAvailable(mapElement, join->GetRightInput()->GetOutputIUs()) && canPushRight) {
            rightMapElements.push_back(mapElement);
            classifiedElements.emplace_back(mapElement, EPushTarget::Right);
        } else {
            classifiedElements.emplace_back(mapElement, EPushTarget::Top);
        }
    }

    if (leftMapElements.empty() && rightMapElements.empty()) {
        return map;
    }

    TIntrusivePtr<TOpMap> leftMap;
    bool pushLeft = !leftMapElements.empty();
    if (!leftMapElements.empty()) {
        leftMap = MakeIntrusive<TOpMap>(join->GetLeftInput(), map->Pos, leftMapElements);
        if (HasDuplicateOutputs(leftMap)) {
            pushLeft = false;
            leftMap = nullptr;
        }
    }

    TIntrusivePtr<TOpMap> rightMap;
    bool pushRight = !rightMapElements.empty();
    if (!rightMapElements.empty()) {
        rightMap = MakeIntrusive<TOpMap>(join->GetRightInput(), map->Pos, rightMapElements);
        if (HasDuplicateOutputs(rightMap)) {
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

    TVector<TMapElement> topMapElements;
    for (const auto& [mapElement, target] : classifiedElements) {
        if (target == EPushTarget::Top ||
            (target == EPushTarget::Left && !pushLeft) ||
            (target == EPushTarget::Right && !pushRight)) {
            topMapElements.push_back(mapElement);
        }
    }

    if (topMapElements.empty()) {
        return join;
    }

    return MakeIntrusive<TOpMap>(join, map->Pos, topMapElements, map->Ordered);
}

} // anonymous namespace

// Push append-only map elements closer to sources. If only some elements can move, leave the rest above.
// Semantic renames are a barrier: they change visible bindings, so this rule does not move them.

TIntrusivePtr<IOperator> TPushMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);
    if (!CanPushMap(map)) {
        return input;
    }

    if (map->GetInput()->Kind == EOperator::Map && map->GetInput()->IsSingleConsumer()) {
        return SinkMapElementsToMap(map);
    }

    if (map->GetInput()->Kind == EOperator::Join) {
        return PushMapThroughJoin(map);
    }

    return input;
}
}
}
