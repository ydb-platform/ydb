#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

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
    return joinKind == "Inner" || joinKind == "Cross" || joinKind == "Right" || joinKind == "RightOnly" || joinKind == "RightSemi";
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
    TVector<std::pair<TMapElement, EPushTarget>> classifiedElements;

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
        }
        classifiedElements.emplace_back(mapElement, target);
    }

    if (leftMapElements.empty() && rightMapElements.empty()) {
        return input;
    }

    bool pushLeft = !leftMapElements.empty();
    bool pushRight = !rightMapElements.empty();

    TVector<TMapElement> topMapElements;
    for (const auto& [mapElement, target] : classifiedElements) {
        if (target == EPushTarget::Top) {
            topMapElements.push_back(mapElement);
        }
    }

    if (topMapElements.empty()) {
        if (pushLeft) {
            auto leftMap = MakeIntrusive<TOpMap>(originalLeftInput, topMap->Pos, leftMapElements);
            join->SetLeftInput(leftMap);
        }
        if (pushRight) {
            auto rightMap = MakeIntrusive<TOpMap>(originalRightInput, topMap->Pos, rightMapElements);
            join->SetRightInput(rightMap);
        }
        return join;
    }

    if (pushLeft) {
        auto leftMap = MakeIntrusive<TOpMap>(originalLeftInput, topMap->Pos, leftMapElements);
        join->SetLeftInput(leftMap);
    }
    if (pushRight) {
        auto rightMap = MakeIntrusive<TOpMap>(originalRightInput, topMap->Pos, rightMapElements);
        join->SetRightInput(rightMap);
    }
    return MakeIntrusive<TOpMap>(join, topMap->Pos, topMapElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
