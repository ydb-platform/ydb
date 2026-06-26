#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ x := l, y := f(r) ]  == becomes ==>  Join
// B: `- Join                                      |- Map [ x := l ]
// C:    |- left                                   |  `- left
// D:    `- right                                  `- Map [ y := f(r) ]
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
// A: Map [ x := l ]         -- move prevented if Join B or the chosen input
// B: `- Join                   has multiple consumers; pushing below it would
// C:    |- left                require cloning the shared node.
// D:    `- right

namespace {

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

EPushTarget SelectAliasJoinPushTarget(
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& leftOutput,
    const TVector<TInfoUnit>& rightOutput)
{
    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    const bool leftAvailable = IUSetDiff(usedIUs, leftOutput).empty();
    const bool rightAvailable = IUSetDiff(usedIUs, rightOutput).empty();
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
    const auto usedIUs = mapElement.GetExpression().GetInputIUs(false, true);
    const bool leftAvailable = IUSetDiff(usedIUs, leftOutput).empty();
    const bool rightAvailable = IUSetDiff(usedIUs, rightOutput).empty();
    const bool leftPreserved = IsLeftPreserved(joinKind);
    const bool rightPreserved = IsRightPreserved(joinKind);

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

} // anonymous namespace

TIntrusivePtr<IOperator> TPushAppendThroughJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

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
    if (!join->IsSingleConsumer() || !originalLeftInput->IsSingleConsumer() || !originalRightInput->IsSingleConsumer()) {
        return input;
    }

    const auto leftOutput = originalLeftInput->GetOutputIUs();
    const auto rightOutput = originalRightInput->GetOutputIUs();

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<TMapElement> topMapElements;

    for (const auto& mapElement : topMap->MapElements) {
        EPushTarget target = EPushTarget::Top;
        const bool isExtractableAppend = topMap->IsExtractableAppend(mapElement);
        if (isExtractableAppend && mapElement.IsColumnAccess()) {
            target = SelectAliasJoinPushTarget(mapElement, leftOutput, rightOutput);
        } else if (isExtractableAppend && !mapElement.IsColumnAccess()) {
            target = SelectExpressionJoinPushTarget(mapElement, leftOutput, rightOutput, join->JoinKind);
        }

        if (target == EPushTarget::Left) {
            leftMapElements.push_back(mapElement);
        } else if (target == EPushTarget::Right) {
            rightMapElements.push_back(mapElement);
        } else {
            topMapElements.push_back(mapElement);
        }
    }

    if (leftMapElements.empty() && rightMapElements.empty()) {
        return input;
    }

    if (!leftMapElements.empty()) {
        auto leftMap = MakeIntrusive<TOpMap>(originalLeftInput, topMap->Pos, leftMapElements);
        join->SetLeftInput(leftMap);
    }
    if (!rightMapElements.empty()) {
        auto rightMap = MakeIntrusive<TOpMap>(originalRightInput, topMap->Pos, rightMapElements);
        join->SetRightInput(rightMap);
    }

    if (topMapElements.empty()) {
        return join;
    }

    return MakeIntrusive<TOpMap>(join, topMap->Pos, topMapElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
