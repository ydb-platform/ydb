#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <util/generic/bitmap.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ a := b, x <- c ] == becomes ==>  Map [ d := e, a := b, x <- c ]
// B: `- Map [ d := e ]
// C:    `- input

// Caveats:
// 1.
// A: Map [ a := b ]                -- move prevented, because map A
// B: `- Map [ b := a ]                can't be evaluated at point B (no "b")
//
// 1b.
// A: Map [ a := f(x) ]             -- move prevented, because "x" at point A is the
// B: `- Map [ x := g(x), _ <- x ]     output of B's element; moving f(x) into B would
//                                     rebind it to B's input "x" and change its value.
//
// 2.
// A: Map [ a := b, e <- a ]        -- elements whose boundary effects interact move
// B: `- Map [ c := d ]                together only when the whole affected group can
//                                     still evaluate against B's input.
//
// 3.
// A: Map [ x <- a, y <- x ]        -- x <- a stays above B when y <- x cannot move;
// B: `- Map [ c := d ]                otherwise y <- x would consume the pushed x
//                                     instead of B's original x output.
//
// Consequence of this behaviour: stacks of maps with movable elements will
// eventually become topologically sorted when this rule runs in a loop.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;
using TElementMask = TDynBitMap;

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

bool CanMoveToBottomInput(const TMapElement& element, const TVector<TInfoUnit>& bottomInputIUs, const TOpMap& bottomMap) {
    if (!element.DependsOnlyOn(bottomInputIUs)) {
        return false;
    }

    if (element.IsRename()) {
        return !bottomMap.HasOutputElement(element.GetRename());
    }

    return !ReferencesBottomElementOutput(element, bottomMap);
}

TElementMask MakeElementMask(size_t elementCount) {
    TElementMask mask;
    mask.Reserve(elementCount);
    return mask;
}

template <typename TPredicate>
bool HasActiveElement(const TVector<TMapElement>& elements, const TElementMask& active, TPredicate predicate) {
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (active.Get(idx) && predicate(elements[idx])) {
            return true;
        }
    }
    return false;
}

template <typename TPredicate>
bool HasResidualElement(const TVector<TMapElement>& elements, const TElementMask& active, TPredicate predicate) {
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (!active.Get(idx) && predicate(elements[idx])) {
            return true;
        }
    }
    return false;
}

template <typename TPredicate>
bool DeselectElements(const TVector<TMapElement>& elements, TElementMask& active, TPredicate predicate) {
    bool changed = false;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (active.Get(idx) && predicate(elements[idx])) {
            active.Reset(idx);
            changed = true;
        }
    }
    return changed;
}

template <typename TPredicate>
auto WithName(TPredicate predicate, TInfoUnit name) {
    return [predicate, name](const auto& element) {
        return predicate(element, name);
    };
}

bool ProducesName(const TMapElement& element, const TInfoUnit& name) {
    return element.GetElementName() == name;
}

bool RenamesFrom(const TMapElement& element, const TInfoUnit& name) {
    return element.IsRename() && element.GetRename() == name;
}

bool ExpressionUsesName(const TMapElement& element, const TInfoUnit& name) {
    if (element.IsRename()) {
        return false;
    }

    for (const auto& input : element.GetExpression().GetInputIUs(false, true)) {
        if (input == name) {
            return true;
        }
    }
    return false;
}

// Shape:
//   Map [ x <- y, z <- y ]
//   `- Map [...]
// Keep renames from the same source on one side of the boundary.
bool PruneSplitRenames(const TVector<TMapElement>& elements, TElementMask& active) {
    bool changed = false;
    for (const auto& element : elements) {
        if (!element.IsRename()) {
            continue;
        }

        const auto name = element.GetRename();
        if (HasResidualElement(elements, active, WithName(RenamesFrom, name))) {
            changed |= DeselectElements(elements, active, WithName(RenamesFrom, name));
        }
    }
    return changed;
}

// Shape:
//   Map [ x := f(a), z := g(x) ]
//   `- Map [...]
// If x := f(a) moves and z := g(x) stays, z binds to the new x.
bool PruneBindingToAnotherVariable(const TVector<TMapElement>& elements, TElementMask& active) {
    bool changed = false;
    for (const auto& element : elements) {
        const auto name = element.GetElementName();
        if (!HasActiveElement(elements, active, WithName(ProducesName, name))) {
            continue;
        }

        if (HasResidualElement(elements, active, WithName(RenamesFrom, name)) ||
            HasResidualElement(elements, active, WithName(ExpressionUsesName, name))) {
            changed |= DeselectElements(elements, active, WithName(ProducesName, name));
        }
    }
    return changed;
}

// Shape:
//   Map [ x := f(a), _ <- x ]
//   `- Map [ x <- y ]
// Do not push a producer into a bottom map that already produces the same IU.
bool PrunePushingBelowShadowingRename(TOpMap& bottomMap, const TVector<TMapElement>& elements, TElementMask& active) {
    bool changed = false;
    for (const auto& element : elements) {
        const auto output = element.GetElementName();
        if (!HasActiveElement(elements, active, WithName(ProducesName, output))) {
            continue;
        }

        if (bottomMap.HasOutputElement(output)) {
            changed |= DeselectElements(elements, active, WithName(ProducesName, output));
        }
    }
    return changed;
}

void PruneUnsafePushableElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, TElementMask& active) {
    bool changed = true;
    while (changed) {
        changed = PruneSplitRenames(elements, active);
        changed |= PruneBindingToAnotherVariable(elements, active);
        changed |= PrunePushingBelowShadowingRename(bottomMap, elements, active);
    }
}

TRenameMap BuildRenameMap(const TVector<TMapElement>& elements, const TElementMask& active) {
    TRenameMap renameMap;
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (!active.Get(idx) || !elements[idx].IsRename()) {
            continue;
        }

        if (elements[idx].GetRename() != elements[idx].GetElementName()) {
            renameMap.emplace(elements[idx].GetRename(), elements[idx].GetElementName());
        }
    }
    return renameMap;
}

} // anonymous namespace

bool TPushMapElementsIntoMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map &&
        input->Children.front()->Kind == EOperator::Map;
}

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

    auto pushed = MakeElementMask(topMap->MapElements.size());

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (CanMoveToBottomInput(mapElement, bottomInputIUs, *bottomMap)) {
            pushed.Set(idx);
        }
    }
    PruneUnsafePushableElements(*bottomMap, topMap->MapElements, pushed);
    const auto renameMap = BuildRenameMap(topMap->MapElements, pushed);

    TVector<TMapElement> topElements;
    // Map(Map(input, bottomElements), topElements) ->
    // Map(input, bottomElements + movable top elements), with non-movable top elements left above.
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (pushed.Get(idx)) {
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
