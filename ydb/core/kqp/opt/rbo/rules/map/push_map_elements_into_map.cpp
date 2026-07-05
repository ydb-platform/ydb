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
using TElementIndexes = TVector<size_t>;
using TElementIndexMap = THashMap<TInfoUnit, TElementIndexes, TInfoUnit::THashFunction>;

struct TPushIndexes {
    TElementIndexMap ProducersByOutput;
    TElementIndexMap RebindersByOutput;
    TElementIndexMap PreserversByOutput;
    TElementIndexMap HidersBySource;
    TElementIndexMap RenameUsersBySource;
    TElementIndexMap ExpressionUsersByInput;
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

void AddIndex(TElementIndexMap& map, const TInfoUnit& iu, size_t idx) {
    map[iu].push_back(idx);
}

TPushIndexes BuildPushIndexes(const TVector<TMapElement>& elements) {
    TPushIndexes indexes;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        const auto& element = elements[idx];
        const auto output = element.GetElementName();
        AddIndex(indexes.ProducersByOutput, output, idx);
        AddIndex(PreservesInputValue(element) ? indexes.PreserversByOutput : indexes.RebindersByOutput, output, idx);

        if (element.IsRename()) {
            const auto source = element.GetRename();
            AddIndex(indexes.RenameUsersBySource, source, idx);
            if (source != output) {
                AddIndex(indexes.HidersBySource, source, idx);
            }
            continue;
        }

        for (const auto& input : element.GetExpression().GetInputIUs(false, true)) {
            AddIndex(indexes.ExpressionUsersByInput, input, idx);
        }
    }

    return indexes;
}

const TElementIndexes* FindIndexes(const TElementIndexMap& map, const TInfoUnit& iu) {
    const auto it = map.find(iu);
    return it == map.end() ? nullptr : &it->second;
}

bool HasActiveIndex(const TElementIndexes* indexes, const TVector<bool>& pushed) {
    if (!indexes) {
        return false;
    }

    for (const auto idx : *indexes) {
        if (pushed[idx]) {
            return true;
        }
    }
    return false;
}

bool HasResidualIndex(const TElementIndexes* indexes, const TVector<bool>& pushed) {
    if (!indexes) {
        return false;
    }

    for (const auto idx : *indexes) {
        if (!pushed[idx]) {
            return true;
        }
    }
    return false;
}

bool DeselectIndexes(const TElementIndexes* indexes, TVector<bool>& pushed) {
    if (!indexes) {
        return false;
    }

    bool changed = false;
    for (const auto idx : *indexes) {
        if (pushed[idx]) {
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

bool DeselectDuplicateOutputProducers(TOpMap& bottomMap, const TVector<TMapElement>& elements, const TPushIndexes& indexes, TVector<bool>& pushed) {
    THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction> counts;
    for (const auto& output : BuildBottomOutputWithPushedElements(bottomMap, elements, pushed)) {
        ++counts[output];
    }

    bool changed = false;
    for (const auto& [output, count] : counts) {
        if (count > 1) {
            changed |= DeselectIndexes(FindIndexes(indexes.ProducersByOutput, output), pushed);
        }
    }
    return changed;
}

void ClosePushableElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, TVector<bool>& pushed) {
    const auto indexes = BuildPushIndexes(elements);

    bool changed = true;
    while (changed) {
        changed = false;

        for (const auto& [source, hiders] : indexes.HidersBySource) {
            if (!HasActiveIndex(&hiders, pushed) ||
                HasActiveIndex(FindIndexes(indexes.PreserversByOutput, source), pushed)) {
                continue;
            }

            if (HasResidualIndex(FindIndexes(indexes.RenameUsersBySource, source), pushed)) {
                changed |= DeselectIndexes(&hiders, pushed);
            }
        }

        for (const auto& [output, rebinders] : indexes.RebindersByOutput) {
            if (!HasActiveIndex(&rebinders, pushed)) {
                continue;
            }

            if (HasResidualIndex(FindIndexes(indexes.RenameUsersBySource, output), pushed) ||
                HasResidualIndex(FindIndexes(indexes.ExpressionUsersByInput, output), pushed)) {
                changed |= DeselectIndexes(FindIndexes(indexes.ProducersByOutput, output), pushed);
            }
        }

        changed |= DeselectDuplicateOutputProducers(bottomMap, elements, indexes, pushed);
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
