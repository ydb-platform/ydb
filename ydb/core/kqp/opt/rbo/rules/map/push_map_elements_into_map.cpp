#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ a := b, x <- c ] == becomes ==>  Map [ d := e, a := b, x <- c ]
// B: `- Map [ d := e ]
// C:    `- input

// Caveats:
// 1.
// A: Map [ a := b ]         -- move prevented, because map A
// B: `- Map [ b := a ]         can't be evaluated at point B (no "b")
//
// 1b.
// A: Map [ a := f(x) ]      -- move prevented, because "x" at point A is the
// B: `- Map [ x := g(x) ]      output of B's element; moving f(x) into B would
//                              rebind it to B's input "x" and change its value.
//
// 2.
// A: Map [ a := b, e <- a ] -- elements whose boundary effects interact move
// B: `- Map [ c := d ]         together only when the whole affected group can
//                              still evaluate against B's input.
//
// 3.
// A: Map [ x <- a, y <- x ] -- x <- a stays above B when y <- x cannot move;
// B: `- Map [ c := d ]         otherwise y <- x would consume the pushed x
//                              instead of B's original x output.
//
// Consequence of this behaviour: stacks of maps with movable elements will
// eventually become topologically sorted when this rule runs in a loop.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

struct TBoundaryChanges {
    TInfoUnitSet HiddenSources;
    TInfoUnitSet ReboundOutputs;
};

// A top element evaluates against the bottom map's output; after the move it
// evaluates against the bottom map's input. The move preserves bindings only
// when no referenced name is (re)defined by a bottom element.
bool ReferencesBottomElementOutput(const TMapElement& element, const TOpMap& bottomMap) {
    for (const auto& iu : element.GetExpression().GetInputIUs(false, true)) {
        if (bottomMap.HasOutputElement(iu)) {
            return true;
        }
    }
    return false;
}

bool PreservesInputValue(const TMapElement& element) {
    if (element.IsRename()) {
        return element.GetRename() == element.GetElementName();
    }

    return element.IsColumnAccess() && element.GetColumnAccess() == element.GetElementName();
}

bool CanMoveToBottomInput(const TMapElement& element, const TVector<TInfoUnit>& bottomInputIUs, const TOpMap& bottomMap) {
    if (!element.DependsOnlyOn(bottomInputIUs)) {
        return false;
    }

    if (element.IsRename()) {
        return !bottomMap.HasOutputElement(element.GetRename());
    }

    return !ReferencesBottomElementOutput(element, bottomMap);
}

TBoundaryChanges GetBoundaryChanges(const TVector<TMapElement>& elements, const TVector<bool>& pushed) {
    TBoundaryChanges changes;
    TInfoUnitSet preservedOutputs;

    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (!pushed[idx]) {
            continue;
        }

        const auto& element = elements[idx];
        if (PreservesInputValue(element)) {
            preservedOutputs.insert(element.GetElementName());
        } else {
            changes.ReboundOutputs.insert(element.GetElementName());
        }
    }

    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (!pushed[idx] || !elements[idx].IsRename()) {
            continue;
        }

        const auto source = elements[idx].GetRename();
        if (!preservedOutputs.contains(source)) {
            changes.HiddenSources.insert(source);
        }
    }

    return changes;
}

bool DeselectProducers(
    const TVector<TMapElement>& elements,
    const TInfoUnit& output,
    TVector<bool>& pushed)
{
    bool changed = false;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (pushed[idx] && elements[idx].GetElementName() == output) {
            pushed[idx] = false;
            changed = true;
        }
    }
    return changed;
}

bool DeselectHiders(
    const TVector<TMapElement>& elements,
    const TInfoUnit& source,
    TVector<bool>& pushed)
{
    bool changed = false;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (pushed[idx] &&
            elements[idx].IsRename() &&
            elements[idx].GetRename() == source &&
            elements[idx].GetRename() != elements[idx].GetElementName()) {
            pushed[idx] = false;
            changed = true;
        }
    }
    return changed;
}

TVector<TInfoUnit> BuildBottomOutputWithPushedElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, const TVector<bool>& pushed) {
    TVector<TInfoUnit> result = bottomMap.GetInput()->GetOutputIUs();
    TInfoUnitSet renameSources = bottomMap.GetRenameSources();

    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (pushed[idx] && elements[idx].IsRename()) {
            renameSources.insert(elements[idx].GetRename());
        }
    }

    if (!renameSources.empty()) {
        TVector<TInfoUnit> kept;
        kept.reserve(result.size());
        for (const auto& iu : result) {
            if (!renameSources.contains(iu)) {
                kept.push_back(iu);
            }
        }
        result = std::move(kept);
    }

    for (const auto& element : bottomMap.MapElements) {
        result.push_back(element.GetElementName());
    }

    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (pushed[idx]) {
            result.push_back(elements[idx].GetElementName());
        }
    }

    return result;
}

bool DeselectDuplicateOutputProducers(TOpMap& bottomMap, const TVector<TMapElement>& elements, TVector<bool>& pushed) {
    THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction> counts;
    for (const auto& output : BuildBottomOutputWithPushedElements(bottomMap, elements, pushed)) {
        ++counts[output];
    }

    bool changed = false;
    for (const auto& [output, count] : counts) {
        if (count > 1) {
            changed |= DeselectProducers(elements, output, pushed);
        }
    }
    return changed;
}

void ClosePushableElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, TVector<bool>& pushed) {
    bool changed = true;
    while (changed) {
        changed = false;
        const auto boundaryChanges = GetBoundaryChanges(elements, pushed);

        for (size_t idx = 0; idx < elements.size(); ++idx) {
            if (pushed[idx]) {
                continue;
            }

            const auto& element = elements[idx];
            if (element.IsRename()) {
                const auto source = element.GetRename();
                if (boundaryChanges.HiddenSources.contains(source)) {
                    changed |= DeselectHiders(elements, source, pushed);
                }
                if (boundaryChanges.ReboundOutputs.contains(source)) {
                    changed |= DeselectProducers(elements, source, pushed);
                }
                continue;
            }

            for (const auto& input : element.GetExpression().GetInputIUs(false, true)) {
                if (boundaryChanges.ReboundOutputs.contains(input)) {
                    changed |= DeselectProducers(elements, input, pushed);
                }
            }
        }

        changed |= DeselectDuplicateOutputProducers(bottomMap, elements, pushed);
    }
}

TRenameMap BuildRenameMap(const TVector<TMapElement>& elements, const TVector<bool>& pushed) {
    TRenameMap renameMap;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (!pushed[idx] || !elements[idx].IsRename()) {
            continue;
        }

        if (elements[idx].GetRename() != elements[idx].GetElementName()) {
            renameMap.emplace(elements[idx].GetRename(), elements[idx].GetElementName());
        }
    }
    return renameMap;
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsIntoMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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

    TVector<bool> pushed(topMap->MapElements.size(), false);

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        pushed[idx] = CanMoveToBottomInput(mapElement, bottomInputIUs, *bottomMap);
    }
    ClosePushableElements(*bottomMap, topMap->MapElements, pushed);
    const auto renameMap = BuildRenameMap(topMap->MapElements, pushed);

    TVector<TMapElement> topElements;
    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top elements), with non-movable top elements left above.
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (pushed[idx]) {
            bottomElements.push_back(mapElement);
            continue;
        }

        topElements.push_back(mapElement);
    }

    if (bottomElements.size() == bottomMap->MapElements.size()) {
        return input;
    }

    for (auto& element : topElements) {
        if (!element.IsRename()) {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
    }
    bottomMap->MapElements = std::move(bottomElements);
    props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);

    if (topElements.empty()) {
        return bottomMap;
    }

    return MakeIntrusive<TOpMap>(bottomMap, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
