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
using TElementMask = TDynBitMap;
using TElementMaskMap = THashMap<TInfoUnit, TElementMask, TInfoUnit::THashFunction>;

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

const TElementMask* FindMask(const TElementMaskMap& map, const TInfoUnit& iu) {
    const auto it = map.find(iu);
    return it == map.end() ? nullptr : &it->second;
}

TElementMask MakeElementMask(size_t elementCount) {
    TElementMask mask;
    mask.Reserve(elementCount);
    return mask;
}

void AddIndex(TElementMaskMap& map, const TInfoUnit& iu, size_t idx, size_t elementCount) {
    auto& mask = map[iu];
    mask.Reserve(elementCount);
    mask.Set(idx);
}

struct TPushIndexes {
    explicit TPushIndexes(size_t elementCount)
        : ElementCount(elementCount)
    {}

    const TElementMask* Produced(const TInfoUnit& iu) const {
        return FindMask(ByProducedName, iu);
    }

    const TElementMask* Preserved(const TInfoUnit& iu) const {
        return FindMask(ByPreservedName, iu);
    }

    const TElementMask* RenameUsers(const TInfoUnit& iu) const {
        return FindMask(RenameUsersByInput, iu);
    }

    const TElementMask* ExpressionUsers(const TInfoUnit& iu) const {
        return FindMask(ExpressionUsersByInput, iu);
    }

    void AddProduced(const TInfoUnit& iu, size_t idx) {
        AddIndex(ByProducedName, iu, idx, ElementCount);
    }

    void AddRebound(const TInfoUnit& iu, size_t idx) {
        AddIndex(ByReboundName, iu, idx, ElementCount);
    }

    void AddPreserved(const TInfoUnit& iu, size_t idx) {
        AddIndex(ByPreservedName, iu, idx, ElementCount);
    }

    void AddHidden(const TInfoUnit& iu, size_t idx) {
        AddIndex(ByHiddenName, iu, idx, ElementCount);
    }

    void AddRenameUser(const TInfoUnit& iu, size_t idx) {
        AddIndex(RenameUsersByInput, iu, idx, ElementCount);
    }

    void AddExpressionUser(const TInfoUnit& iu, size_t idx) {
        AddIndex(ExpressionUsersByInput, iu, idx, ElementCount);
    }

    size_t ElementCount;
    TElementMaskMap ByProducedName;
    TElementMaskMap ByReboundName;
    TElementMaskMap ByPreservedName;
    TElementMaskMap ByHiddenName;
    TElementMaskMap RenameUsersByInput;
    TElementMaskMap ExpressionUsersByInput;
};

TPushIndexes BuildPushIndexes(const TVector<TMapElement>& elements) {
    TPushIndexes indexes(elements.size());
    for (size_t idx = 0; idx < elements.size(); ++idx) {
        const auto& element = elements[idx];
        const auto output = element.GetElementName();
        indexes.AddProduced(output, idx);
        if (PreservesInputValue(element)) {
            indexes.AddPreserved(output, idx);
        } else {
            indexes.AddRebound(output, idx);
        }

        if (element.IsRename()) {
            const auto source = element.GetRename();
            indexes.AddRenameUser(source, idx);
            if (source != output) {
                indexes.AddHidden(source, idx);
            }
            continue;
        }

        for (const auto& input : element.GetExpression().GetInputIUs(false, true)) {
            indexes.AddExpressionUser(input, idx);
        }
    }

    return indexes;
}

bool HasActive(const TElementMask* mask, const TElementMask& active) {
    return mask && active.HasAny(*mask);
}

bool HasResidual(const TElementMask* mask, const TElementMask& active) {
    return mask && !active.HasAll(*mask);
}

bool Deselect(const TElementMask* mask, TElementMask& active) {
    if (!mask) {
        return false;
    }

    const bool changed = active.HasAny(*mask);
    active -= *mask;
    return changed;
}

// Shape:
//   Map [ x <- y, z <- y ]
//   `- Map [...]
// If x <- y moves and z <- y stays, y is hidden below the boundary.
bool PruneSourceHidingRenamesWithResidualRenames(const TPushIndexes& indexes, TElementMask& active) {
    bool changed = false;
    for (const auto& [name, hiders] : indexes.ByHiddenName) {
        if (!HasActive(&hiders, active) ||
            HasActive(indexes.Preserved(name), active)) {
            continue;
        }

        if (HasResidual(indexes.RenameUsers(name), active)) {
            changed |= Deselect(&hiders, active);
        }
    }
    return changed;
}

// Shape:
//   Map [ x := f(a), z := g(x) ]
//   `- Map [...]
// If x := f(a) moves and z := g(x) stays, z binds to the new x.
bool PruneReboundOutputsWithResidualUsers(const TPushIndexes& indexes, TElementMask& active) {
    bool changed = false;
    for (const auto& [name, rebinders] : indexes.ByReboundName) {
        if (!HasActive(&rebinders, active)) {
            continue;
        }

        if (HasResidual(indexes.RenameUsers(name), active) ||
            HasResidual(indexes.ExpressionUsers(name), active)) {
            changed |= Deselect(indexes.Produced(name), active);
        }
    }
    return changed;
}

TVector<TInfoUnit> BuildBottomOutputWithPushedElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, const TElementMask& active) {
    TVector<TInfoUnit> result = bottomMap.GetInput()->GetOutputIUs();
    TInfoUnitSet renameSources = bottomMap.GetRenameSources();

    for (size_t idx = 0; idx < elements.size(); ++idx) {
        if (active.Get(idx) && elements[idx].IsRename()) {
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
        if (active.Get(idx)) {
            result.push_back(elements[idx].GetElementName());
        }
    }

    return result;
}

// Shape:
//   Map [ x := f(a) ]
//   `- Map [...]  // bottom output already has x
// Pushing x must not leave duplicate x in bottom output.
bool PruneDuplicateBottomOutputs(TOpMap& bottomMap, const TVector<TMapElement>& elements, const TPushIndexes& indexes, TElementMask& active) {
    THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction> counts;
    for (const auto& output : BuildBottomOutputWithPushedElements(bottomMap, elements, active)) {
        ++counts[output];
    }

    bool changed = false;
    for (const auto& [output, count] : counts) {
        if (count > 1) {
            changed |= Deselect(indexes.Produced(output), active);
        }
    }
    return changed;
}

void ClosePushableElements(TOpMap& bottomMap, const TVector<TMapElement>& elements, TElementMask& active) {
    const auto indexes = BuildPushIndexes(elements);

    bool changed = true;
    while (changed) {
        changed = PruneSourceHidingRenamesWithResidualRenames(indexes, active);
        changed |= PruneReboundOutputsWithResidualUsers(indexes, active);
        changed |= PruneDuplicateBottomOutputs(bottomMap, elements, indexes, active);
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
    ClosePushableElements(*bottomMap, topMap->MapElements, pushed);
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
