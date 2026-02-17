#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {
    
// We push the map operator only below join right now
// We only push a non-projecting map operator, and there are some limitations to where we can push:
//  - we cannot push the right side of left join for example or left side of right join

std::shared_ptr<IOperator> TPushMapRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);
    if (map->Project) {
        return input;
    }

    if (map->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(map->GetInput());
    bool canPushRight = join->JoinKind != "Left" && join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";
    bool canPushLeft = join->JoinKind != "Right" && join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";

    // Make sure the join and its inputs are single consumer
    // FIXME: join inputs don't have to be single consumer, but this used to break due to mutliple consumer problem
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return input;
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<TMapElement> topMapElements;
    TVector<int> removeElements;

    for (size_t i = 0; i < map->MapElements.size(); i++) {
        const auto & mapElement = map->MapElements[i];
        auto mapElIUs = mapElement.GetExpression().GetInputIUs(false, true);

        if (!IUSetDiff(mapElIUs, join->GetLeftInput()->GetOutputIUs()).size() && canPushLeft) {
            leftMapElements.push_back(mapElement);
        } else if (!IUSetDiff(mapElIUs, join->GetRightInput()->GetOutputIUs()).size() && canPushRight) {
            rightMapElements.push_back(mapElement);
        } else {
            topMapElements.push_back(mapElement);
        }
    }

    if (!leftMapElements.size() && !rightMapElements.size()) {
        return input;
    }

    std::shared_ptr<IOperator> output;
    if (!topMapElements.size()) {
        output = join;
    } else {
        output = std::make_shared<TOpMap>(join, input->Pos, topMapElements, false);
    }

    if (leftMapElements.size()) {
        auto leftInput = join->GetLeftInput();
        join->SetLeftInput(std::make_shared<TOpMap>(leftInput, input->Pos, leftMapElements, false));
    }

    if (rightMapElements.size()) {
        auto rightInput = join->GetRightInput();
        join->SetRightInput(std::make_shared<TOpMap>(rightInput, input->Pos, rightMapElements, false));
    }

    return output;
}
}
}