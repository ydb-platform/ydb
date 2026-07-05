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
// A: Map [ a := b, e <- a ] -- move prevented, because "a := b" produces
// B: `- Map [ c := d ]         "a", which would be removed from output by
//                              rename "e <- a" if it's moved below.
//
// 3.
// A: Map [ x <- a, y <- x ] -- move prevented, because pushing x <- a into B
// B: `- Map [ c := d ]         would make y <- x hide x above B.
//
// Consequence of this behaviour: stacks of maps with movable elements will
// eventually become topologically sorted when this rule runs in a loop.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

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

bool HasResidualRenameFromChangedName(
    const TVector<TMapElement>& elements,
    const TVector<bool>& pushedRenames,
    const TInfoUnitSet& changedNames)
{
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (pushedRenames[idx]) {
            continue;
        }

        const auto& element = elements[idx];
        if (element.IsRename() && changedNames.contains(element.GetRename())) {
            return true;
        }
    }
    return false;
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

    TVector<bool> pushedRenames(topMap->MapElements.size(), false);
    TRenameMap renameMap;
    TInfoUnitSet changedNames;

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (!mapElement.IsRename() ||
            !mapElement.DependsOnlyOn(bottomInputIUs) ||
            bottomMap->HasOutputElement(mapElement.GetRename())) {
            continue;
        }

        pushedRenames[idx] = true;
        changedNames.insert(mapElement.GetRename());
        changedNames.insert(mapElement.GetElementName());
        if (mapElement.GetRename() != mapElement.GetElementName()) {
            renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
        }
    }

    if (HasResidualRenameFromChangedName(topMap->MapElements, pushedRenames, changedNames)) {
        pushedRenames.assign(topMap->MapElements.size(), false);
        renameMap.clear();
    }

    TVector<TMapElement> topElements;
    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top elements), with non-movable top elements left above.
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (pushedRenames[idx]) {
            bottomElements.push_back(mapElement);
            continue;
        }

        if (!topMap->IsExtractableAppend(mapElement) ||
            !mapElement.DependsOnlyOn(bottomInputIUs) ||
            ReferencesBottomElementOutput(mapElement, *bottomMap)) {
            topElements.push_back(mapElement);
            continue;
        }

        bottomElements.push_back(mapElement);
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
