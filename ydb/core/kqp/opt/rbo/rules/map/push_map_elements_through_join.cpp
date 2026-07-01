#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ x := l, y <- r ]      == becomes ==>  Join
// B: `- Join                                      |- Map [ x := l ]
// C:    |- left                                   |  `- left
// D:    `- right                                  `- Map [ y <- r ]
// E:                                                  `- right
//
// Caveats:
// 1.
// A: Map [ x := f(l) ]      -- expression appends move only below a side
// B: `- Join                   whose rows are preserved by the join. After
// C:    |- left                right joins are rewritten, the right side is
// D:    `- right               preserved only for Inner/Cross joins.
//
// 2.
// A: Map [ a := f(l), x <- a ] -- rename x <- a stays above Join B if a := f(l)
// B: `- Join                     stays above it; otherwise x would rename the
// C:    |- left                  input name that the top expression still needs.
// D:    `- right
//
// 3.
// A: Map [ x := l ]         -- move prevented if Join B has multiple consumers;
// B: `- Join B                 otherwise changing B's inputs would also affect
// C:    |- left                Parent2.
// D:    `- right
// E: Parent2
// F: `- Join B

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

enum class EPushTarget {
    Top,
    Left,
    Right
};

bool IsLeftPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross" || joinKind == "Left" || joinKind == "LeftOnly" || joinKind == "LeftSemi";
}

bool IsRightPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross";
}

bool IsPreserved(EPushTarget target, const TString& joinKind) {
    switch (target) {
        case EPushTarget::Left:
            return IsLeftPreserved(joinKind);
        case EPushTarget::Right:
            return IsRightPreserved(joinKind);
        case EPushTarget::Top:
            return false;
    }
}

EPushTarget SelectJoinPushTarget(
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& leftOutput,
    const TVector<TInfoUnit>& rightOutput)
{
    const bool leftAvailable = mapElement.DependsOnlyOn(leftOutput);
    const bool rightAvailable = mapElement.DependsOnlyOn(rightOutput);
    if (leftAvailable == rightAvailable) {
        return EPushTarget::Top;
    }

    return leftAvailable ? EPushTarget::Left : EPushTarget::Right;
}

EPushTarget SelectExpressionJoinPushTarget(
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& leftOutput,
    const TVector<TInfoUnit>& rightOutput,
    const TString& joinKind)
{
    const auto target = SelectJoinPushTarget(mapElement, leftOutput, rightOutput);
    if (target != EPushTarget::Top) {
        return IsPreserved(target, joinKind) ? target : EPushTarget::Top;
    }

    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    if (usedIUs.empty()) {
        if (IsLeftPreserved(joinKind)) {
            return EPushTarget::Left;
        }
        if (IsRightPreserved(joinKind)) {
            return EPushTarget::Right;
        }
    }

    return EPushTarget::Top;
}

} // anonymous namespace

TIntrusivePtr<IOperator> TPushMapElementsThroughJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(topMap->GetInput());
    const auto originalLeftInput = join->GetLeftInput();
    const auto originalRightInput = join->GetRightInput();
    if (!join->IsSingleConsumer()) {
        return input;
    }

    const auto leftOutput = originalLeftInput->GetOutputIUs();
    const auto rightOutput = originalRightInput->GetOutputIUs();
    THashMap<TInfoUnit, EPushTarget, TInfoUnit::THashFunction> appendTargets;

    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename()) {
            continue;
        }

        const auto target = mapElement.IsColumnAccess()
            ? SelectJoinPushTarget(mapElement, leftOutput, rightOutput)
            : SelectExpressionJoinPushTarget(mapElement, leftOutput, rightOutput, join->JoinKind);
        appendTargets.emplace(mapElement.GetElementName(), target);
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<TMapElement> topMapElements;
    TRenameMap renameMap;

    for (const auto& mapElement : topMap->MapElements) {
        EPushTarget target = EPushTarget::Top;

        if (mapElement.IsRename()) {
            const auto appendTarget = appendTargets.find(mapElement.GetRename());
            target = appendTarget != appendTargets.end()
                ? appendTarget->second
                : SelectJoinPushTarget(mapElement, leftOutput, rightOutput);
        } else {
            target = appendTargets.at(mapElement.GetElementName());
        }

        if (target == EPushTarget::Left) {
            leftMapElements.push_back(mapElement);
        } else if (target == EPushTarget::Right) {
            rightMapElements.push_back(mapElement);
        } else {
            topMapElements.push_back(mapElement);
            continue;
        }

        if (mapElement.IsRename() && mapElement.GetRename() != mapElement.GetElementName()) {
            renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
        }
    }

    if (leftMapElements.empty() && rightMapElements.empty()) {
        return input;
    }

    if (!renameMap.empty()) {
        for (auto& mapElement : topMapElements) {
            if (!mapElement.IsRename()) {
                mapElement.SetExpression(mapElement.GetExpression().ApplyRenames(renameMap));
            }
        }
    }

    if (!leftMapElements.empty()) {
        auto leftMap = MakeIntrusive<TOpMap>(originalLeftInput, topMap->Pos, leftMapElements);
        join->SetLeftInput(leftMap);
    }
    if (!rightMapElements.empty()) {
        auto rightMap = MakeIntrusive<TOpMap>(originalRightInput, topMap->Pos, rightMapElements);
        join->SetRightInput(rightMap);
    }

    if (!renameMap.empty()) {
        join->RenameUsedIUs(renameMap, ctx.ExprCtx);
        props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);
    }

    if (topMapElements.empty()) {
        return join;
    }

    return MakeIntrusive<TOpMap>(join, topMap->Pos, topMapElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
