#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ a := b ]         == becomes ==>  Map [ c := d, a := b ]
// B: `- Map [ c := d ]      (element "a := b" is pushed into map B)

// Caveats:
// 1.
// A: Map [ a := b ]         -- move prevented, because map A
// B: `- Map [ b := a ]         can't be evaluated at point B (no "b")
//
// 2.
// A: Map [ a := b, e <- a ] -- move prevented, because "a := b" produces
// B: `- Map [ c := d ]         "a", which would be removed from output by
//                              rename "e <- a" if it's moved below.
//
// Consequence of this behaviour #1: stacks of maps with just appends
// will eventually become topologically sorted (when this rule runs in
// a loop, e.g. inside a rule stage)

TIntrusivePtr<IOperator>
TPushAppendIntoMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->GetInput()->Kind != EOperator::Map || !topMap->GetInput()->IsSingleConsumer()) {
        return input;
    }

    auto bottomMap = CastOperator<TOpMap>(topMap->GetInput());
    const auto bottomInputIUs = bottomMap->GetInput()->GetOutputIUs();
    auto bottomElements = bottomMap->MapElements;

    TVector<TMapElement> topElements;
    bool pushed = false;

    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top appends), with non-movable top elements left above.
    for (const auto& mapElement : topMap->MapElements) {
        if (!topMap->IsExtractableAppend(mapElement) || !mapElement.DependsOnlyOn(bottomInputIUs)) {
            topElements.push_back(mapElement);
            continue;
        }

        bottomElements.push_back(mapElement);
        pushed = true;
    }

    if (!pushed) {
        return input;
    }

    bottomMap->MapElements = std::move(bottomElements);
    if (topElements.empty()) {
        return bottomMap;
    }

    return MakeIntrusive<TOpMap>(bottomMap, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
