#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shapes this handles:
// A: Map [ a := f(b) ]      == becomes ==>  Filter
// B: `- Filter                               `- Map [ a := f(b) ]
// C:    `- input                                `- input
//
// A: Map [ x := f(l), y := g(r) ]  == becomes ==>  Join
// B: `- Join                                        |- Map [ x := f(l) ]
// C:    |- left                                     |  `- left
// D:    `- right                                    `- Map [ y := g(r) ]
// E:                                                   `- right
//
// Copies move through Filter, Limit, Sort and Join. Computations move through
// Filter, and below a Join only to a side whose rows are preserved. A constant
// moves to the first preserved Join side.

namespace {

bool CanPushMapThroughInput(const IOperator& op) {
    switch (op.Kind) {
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::Join:
            return true;
        case EOperator::Map:
            return CastOperator<TOpMap>(op).NeedToPush == false;
        default:
            return false;
    }
}

bool IsJoinChildPreserved(const TOpJoin& join, ui32 childIdx) {
    const auto& kind = join.JoinKind;
    if (kind == "Inner" || kind == "Cross") {
        return true;
    }
    return childIdx == 0 && (kind == "Left" || kind == "LeftOnly" || kind == "LeftSemi");
}

bool CanPushMapElementToChild(IOperator& op, ui32 childIdx, const TMapElement& element) {
    const bool dependsOnlyOnChild = element.DependsOnlyOn(op.GetChild(childIdx)->GetOutputIUs());
    if (element.IsColumnAccess()) {
        return dependsOnlyOnChild;
    }
    if (op.Kind != EOperator::Join) {
        return op.Kind == EOperator::Filter && dependsOnlyOnChild;
    }
    if (!IsJoinChildPreserved(CastOperator<TOpJoin>(op), childIdx)) {
        return false;
    }
    return dependsOnlyOnChild || element.GetExpression().GetInputIUs(false, true).Empty();
}

} // anonymous namespace

// A: Map [ x := f(a) ]        == becomes ==>  Map [ y := g(b), x := f(a) ]
// B: `- Map [ y := g(b) ]                      `- input
// C:    `- input
//
// A definition moves when it uses only the lower Map's input; definitions that
// use the lower Map's outputs stay above.
bool TPushMapElementsIntoMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map && input->GetChild(0)->Kind == EOperator::Map;
}

TIntrusivePtr<IOperator> TPushMapElementsIntoMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);
    if (!QuickMatch(input)) {
        return input;
    }

    auto& top = CastOperator<TOpMap>(*input);
    auto& bottom = CastOperator<TOpMap>(*top.GetInput());
    const auto available = bottom.GetInput()->GetOutputIUs();
    TMapIUs kept;
    bool moved = false;
    for (const auto id : top.GetMapElements().Keys()) {
        const auto& element = *top.GetMapElements().Find(id);
        if (element.DependsOnlyOn(available)) {
            bottom.AddMapElement(id, element);
            moved = true;
        } else {
            kept.Add(id, element);
        }
    }

    if (!moved) {
        return input;
    }
    if (kept.Keys().Empty()) {
        return top.GetInput();
    }
    return MakeIntrusive<TOpMap>(top.GetInput(), top.Pos, std::move(kept));
}

bool TPushMapElementsThroughInputRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map && CastOperator<TOpMap>(input)->NeedToPush && CanPushMapThroughInput(*input->GetChild(0));
}

TIntrusivePtr<IOperator> TPushMapElementsThroughInputRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);
    if (!QuickMatch(input)) {
        return input;
    }

    auto& map = CastOperator<TOpMap>(*input);
    if (!map.NeedToPush) {
        return input;
    }

    auto& op = *map.GetInput();
    TVector<TMapIUs> pushed(op.GetChildCount());
    TMapIUs kept;
    bool hasPushed = false;
    for (const auto id : map.GetMapElements().Keys()) {
        const auto& element = *map.GetMapElements().Find(id);
        ui32 childIdx = 0;
        while (childIdx < op.GetChildCount() && !CanPushMapElementToChild(op, childIdx, element)) {
            ++childIdx;
        }
        if (childIdx == op.GetChildCount()) {
            kept.Add(id, element);
        } else {
            pushed[childIdx].Add(id, element);
            hasPushed = true;
        }
    }

    if (!hasPushed) {
        return input;
    }

    for (ui32 childIdx = 0; childIdx < pushed.size(); ++childIdx) {
        if (!pushed[childIdx].Keys().Empty()) {
            op.SetChild(childIdx, MakeIntrusive<TOpMap>(op.GetChild(childIdx), map.Pos, std::move(pushed[childIdx]), true));
        }
    }
    op.Props.OutputIUs.reset();

    if (kept.Keys().Empty()) {
        return map.GetInput();
    }
    return MakeIntrusive<TOpMap>(map.GetInput(), map.Pos, std::move(kept));
}

} // namespace NKqp
} // namespace NKikimr
