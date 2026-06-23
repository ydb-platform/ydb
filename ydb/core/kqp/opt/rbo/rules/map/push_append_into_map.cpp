#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool TryAppendToBottomMap(const TIntrusivePtr<TOpMap>& bottomMap, const TMapElement& mapElement,
                          const TVector<TInfoUnit>& bottomInputIUs, const TPlanProps& props) {
    // Move an append into the bottom map only when it can be evaluated before that map.
    if (!mapElement.DependsOnlyOn(bottomInputIUs)) {
        return false;
    }

    auto elements = bottomMap->MapElements;
    elements.push_back(mapElement);

    auto output = BuildMapOutput(bottomMap, elements);
    if (!CanExposeOutput(bottomMap, output, props)) {
        return false;
    }

    bottomMap->MapElements = std::move(elements);
    bottomMap->Props.OutputIUs = std::move(output);
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
    auto originalBottomElements = bottomMap->MapElements;
    auto originalBottomOutput = bottomMap->Props.OutputIUs;

    TVector<TMapElement> topElements;
    bool pushed = false;

    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top appends), with non-movable top elements left above.
    for (const auto& mapElement : topMap->MapElements) {
        if (topMap->IsExtractableAppend(mapElement) && TryAppendToBottomMap(bottomMap, mapElement, bottomInputIUs, props)) {
            pushed = true;
        } else {
            topElements.push_back(mapElement);
        }
    }

    if (!pushed) {
        return input;
    }

    if (topElements.empty()) {
        if (!CanReplaceInParents(topMap, bottomMap, props)) {
            bottomMap->MapElements = std::move(originalBottomElements);
            bottomMap->Props.OutputIUs = std::move(originalBottomOutput);
            return input;
        }
        return bottomMap;
    }

    return MakeIntrusive<TOpMap>(bottomMap, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
