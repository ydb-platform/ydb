#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool TryAppendToBottomMap(
    const TMapElement& mapElement,
    const TVector<TInfoUnit>& bottomInputIUs,
    TVector<TMapElement>& bottomElements,
    TVector<TInfoUnit>& bottomOutput)
{
    // Move an append into the bottom map only when it can be evaluated before that map.
    if (!mapElement.DependsOnlyOn(bottomInputIUs)) {
        return false;
    }

    auto elements = bottomElements;
    elements.push_back(mapElement);

    auto output = BuildMapOutput(bottomInputIUs, elements);
    if (MakeInfoUnitSet(output).size() != output.size()) {
        return false;
    }

    bottomElements = std::move(elements);
    bottomOutput = std::move(output);
    return true;
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushAppendIntoMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

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
    auto bottomOutput = bottomMap->GetOutputIUs();

    TVector<TMapElement> topElements;
    bool pushed = false;

    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top appends), with non-movable top elements left above.
    for (const auto& mapElement : topMap->MapElements) {
        if (topMap->IsExtractableAppend(mapElement) &&
            TryAppendToBottomMap(mapElement, bottomInputIUs, bottomElements, bottomOutput)) {
            pushed = true;
        } else {
            topElements.push_back(mapElement);
        }
    }

    if (!pushed) {
        return input;
    }

    if (topElements.empty()) {
        if (!IUSetIntersect(bottomOutput, GetForbidden(props, topMap.get())).empty()) {
            return input;
        }
        bottomMap->MapElements = std::move(bottomElements);
        bottomMap->Props.OutputIUs = bottomOutput;
        return bottomMap;
    }

    const auto newTopOutput = BuildMapOutput(bottomOutput, topElements);
    if (MakeInfoUnitSet(newTopOutput).size() != newTopOutput.size() ||
        !IUSetIntersect(newTopOutput, GetForbidden(props, topMap.get())).empty()) {
        return input;
    }

    bottomMap->MapElements = std::move(bottomElements);
    bottomMap->Props.OutputIUs = bottomOutput;
    auto newTopMap = MakeIntrusive<TOpMap>(bottomMap, topMap->Pos, topElements, topMap->Ordered);
    newTopMap->Props.OutputIUs = newTopOutput;
    return newTopMap;
}

} // namespace NKqp
} // namespace NKikimr
