#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ a := b ]         == becomes ==>  Unary
// B: `- Unary                              `- Map [ a := b ]
// C:    `- input                              `- input
//
// Where: Unary = either Filter, Limit, Sort or AddDependencies

// Caveats:
// 1.
// A: Map [ a := b ]         -- move prevented, because map A
// B: `- Unary                  can't be evaluated at point C (no "b")
// C:    `- input
//
// 2.
// A: Map [ a := f(b) ]      -- expression appends move only through filters
// B: `- Sort                   when push-under-filter is enabled; alias appends
// C:    `- input               may move through Limit, Sort, and AddDependencies.
//
// 3.
// A: Map [ a := b ]         -- move prevented if Unary B has multiple consumers;
// B: `- Unary B                pushing below B would require cloning Unary B to
// C:    `- input               avoid changing Parent2's input.
// D: Parent2
// E: `- Unary B

TIntrusivePtr<IOperator>
TPushAppendThroughUnaryRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto unaryKind = topMap->GetInput()->Kind;

    const bool canPushThroughFilter = unaryKind == EOperator::Filter && PushUnderFilter;
    const bool canPushAliasThroughUnary =
        unaryKind == EOperator::Limit ||
        unaryKind == EOperator::Sort ||
        unaryKind == EOperator::AddDependencies;

    if (!canPushThroughFilter && !canPushAliasThroughUnary) {
        return input;
    }

    auto unary = CastOperator<IUnaryOperator>(topMap->GetInput());
    if (!unary->IsSingleConsumer()) {
        return input;
    }

    const auto unaryInput = unary->GetInput();
    const auto inputIUs = unaryInput->GetOutputIUs();

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;

    for (const auto& mapElement : topMap->MapElements) {
        const bool isExtractableAppend = topMap->IsExtractableAppend(mapElement);
        const bool canMove = isExtractableAppend &&
            (canPushThroughFilter ||
             (canPushAliasThroughUnary && mapElement.IsColumnAccess()));

        if (canMove && mapElement.DependsOnlyOn(inputIUs)) {
            pushedElements.push_back(mapElement);
            continue;
        }
        topElements.push_back(mapElement);
    }

    if (pushedElements.empty()) {
        return input;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(unaryInput, topMap->Pos, pushedElements);
    unary->SetInput(pushedMap);

    if (topElements.empty()) {
        return unary;
    }

    return MakeIntrusive<TOpMap>(unary, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
