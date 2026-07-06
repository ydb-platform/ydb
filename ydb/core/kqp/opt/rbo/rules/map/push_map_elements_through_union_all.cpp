#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shapes this handles:
// A: Map [ x <- a, y <- b ]  == becomes ==>  UnionAll [ x, y ]
// B: `- UnionAll [ a, b ]                       |- Map [ x <- a, y <- b ]
// C:    |- left                                 |  `- left
// D:    `- right                                `- Map [ x <- a, y <- b ]
// E:                                                 `- right
//
// A: Map [ x <- a, z := f(a) ]  == becomes ==>  Map [ z := f(x) ]
// B: `- UnionAll [ a, b ]                         `- UnionAll [ x, b ]
// C:    |- left                                      |- Map [ x <- a ]
// D:    `- right                                     |  `- left
// E:                                                 `- Map [ x <- a ]
// F:                                                    `- right
//
// The rule pushes semantic renames, including column-access appends whose
// source is dead above the map — such appends are renames in disguise, since
// hiding the dead source changes nothing for consumers. Appends with a live
// source and other expressions stay above UnionAll; moving computation below
// sequence extension can change type alignment between branches.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

bool TryAddColumn(TVector<TInfoUnit>& columns, TInfoUnitSet& columnSet, const TInfoUnit& column) {
    if (columnSet.contains(column)) {
        return false;
    }

    columns.push_back(column);
    columnSet.insert(column);
    return true;
}

TInfoUnitSet GetRenameSources(const TVector<TMapElement>& mapElements) {
    TInfoUnitSet result;
    for (const auto& mapElement : mapElements) {
        if (mapElement.IsRename()) {
            result.insert(mapElement.GetRename());
        }
    }
    return result;
}

bool BuildMapOutput(
    const TVector<TInfoUnit>& inputColumns,
    const TVector<TMapElement>& mapElements,
    TVector<TInfoUnit>* outputColumns = nullptr)
{
    const auto renameSources = GetRenameSources(mapElements);

    TVector<TInfoUnit> result;
    result.reserve(inputColumns.size() + mapElements.size());
    TInfoUnitSet resultSet;
    for (const auto& column : inputColumns) {
        if (!renameSources.contains(column) && !TryAddColumn(result, resultSet, column)) {
            return false;
        }
    }

    for (const auto& mapElement : mapElements) {
        if (!TryAddColumn(result, resultSet, mapElement.GetElementName())) {
            return false;
        }
    }

    if (outputColumns) {
        *outputColumns = std::move(result);
    }
    return true;
}

bool BuildRenamedOutput(
    const TVector<TInfoUnit>& inputColumns,
    const TInfoUnitSet& renameSources,
    const TVector<TMapElement>& mapElements,
    TVector<TInfoUnit>* outputColumns = nullptr)
{
    for (const auto& mapElement : mapElements) {
        if (!ContainsInfoUnit(inputColumns, mapElement.GetRename())) {
            return false;
        }
    }

    TVector<TInfoUnit> result;
    result.reserve(inputColumns.size() + mapElements.size());
    TInfoUnitSet resultSet;
    for (const auto& column : inputColumns) {
        if (!renameSources.contains(column) && !TryAddColumn(result, resultSet, column)) {
            return false;
        }
    }

    for (const auto& mapElement : mapElements) {
        if (!TryAddColumn(result, resultSet, mapElement.GetElementName())) {
            return false;
        }
    }

    if (outputColumns) {
        *outputColumns = std::move(result);
    }
    return true;
}

void RecomputeChangedNames(
    const TVector<TMapElement>& mapElements,
    const TVector<bool>& pushed,
    TInfoUnitSet& changedNames)
{
    changedNames.clear();
    for (size_t idx = 0; idx < mapElements.size(); ++idx) {
        if (!pushed[idx]) {
            continue;
        }

        const auto& mapElement = mapElements[idx];
        changedNames.insert(mapElement.GetRename());
        changedNames.insert(mapElement.GetElementName());
    }
}

bool RemoveCandidateWithName(
    const TVector<TMapElement>& mapElements,
    const TInfoUnit& name,
    TVector<bool>& pushed)
{
    bool changed = false;
    for (size_t idx = 0; idx < mapElements.size(); ++idx) {
        if (!pushed[idx]) {
            continue;
        }

        const auto& mapElement = mapElements[idx];
        if (mapElement.GetRename() == name || mapElement.GetElementName() == name) {
            pushed[idx] = false;
            changed = true;
        }
    }
    return changed;
}

void RemoveCandidatesReferencedByResidualRenames(
    const TVector<TMapElement>& mapElements,
    TVector<bool>& pushed)
{
    TInfoUnitSet changedNames;
    bool changed = true;
    while (changed) {
        changed = false;
        RecomputeChangedNames(mapElements, pushed, changedNames);

        for (size_t idx = 0; idx < mapElements.size(); ++idx) {
            if (pushed[idx]) {
                continue;
            }

            const auto& mapElement = mapElements[idx];
            if (mapElement.IsRename() &&
                changedNames.contains(mapElement.GetRename()) &&
                RemoveCandidateWithName(mapElements, mapElement.GetRename(), pushed)) {
                changed = true;
            }
        }
    }
}

void ApplyRenamesToResidualExpressions(TVector<TMapElement>& residualElements, const TRenameMap& renameMap) {
    if (renameMap.empty()) {
        return;
    }

    for (auto& mapElement : residualElements) {
        if (!mapElement.IsRename()) {
            mapElement.SetExpression(mapElement.GetExpression().ApplyRenames(renameMap));
        }
    }
}

bool ResidualIsValid(
    const TVector<TInfoUnit>& unionColumns,
    const TInfoUnitSet& pushedRenameSources,
    const TVector<TMapElement>& residualElements)
{
    for (const auto& mapElement : residualElements) {
        if (mapElement.IsRename()) {
            if (!ContainsInfoUnit(unionColumns, mapElement.GetRename())) {
                return false;
            }
            continue;
        }

        for (const auto& iu : mapElement.GetExpression().GetInputIUs(false, true)) {
            if (pushedRenameSources.contains(iu)) {
                return false;
            }
        }
    }

    return BuildMapOutput(unionColumns, residualElements);
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->MapElements.empty()) {
        return input;
    }

    if (topMap->GetInput()->Kind != EOperator::UnionAll) {
        return input;
    }

    auto unionAll = CastOperator<TOpUnionAll>(topMap->GetInput());
    if (!unionAll->IsSingleConsumer()) {
        return input;
    }

    const auto originalUnionColumns = unionAll->Columns;
    const auto& liveOut = GetLiveOut(topMap.get());

    // A column-access append whose source is dead above the map is a semantic
    // rename in disguise: hiding the source changes nothing for consumers.
    // Normalize such appends to renames so one push covers both spellings.
    auto mapElements = topMap->MapElements;
    for (auto& mapElement : mapElements) {
        if (!mapElement.IsRename() &&
            mapElement.IsColumnAccess() &&
            mapElement.GetColumnAccess() != mapElement.GetElementName() &&
            !liveOut.contains(mapElement.GetColumnAccess()) &&
            liveOut.contains(mapElement.GetElementName())) {
            mapElement.SetIsRename(true);
        }
    }

    TVector<bool> pushed(mapElements.size(), false);
    for (size_t idx = 0; idx < mapElements.size(); ++idx) {
        const auto& mapElement = mapElements[idx];
        if (!mapElement.IsRename()) {
            continue;
        }

        const auto from = mapElement.GetRename();
        if (from != mapElement.GetElementName() && ContainsInfoUnit(originalUnionColumns, from)) {
            pushed[idx] = true;
        }
    }

    RemoveCandidatesReferencedByResidualRenames(mapElements, pushed);

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> residualElements;
    pushedElements.reserve(mapElements.size());
    residualElements.reserve(mapElements.size());
    TInfoUnitSet pushedRenameSources;
    TRenameMap renameMap;

    for (size_t idx = 0; idx < mapElements.size(); ++idx) {
        const auto& mapElement = mapElements[idx];
        if (!pushed[idx]) {
            residualElements.push_back(mapElement);
            continue;
        }

        pushedElements.push_back(mapElement);
        pushedRenameSources.insert(mapElement.GetRename());
        if (!renameMap.contains(mapElement.GetRename())) {
            renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
        }
    }

    if (pushedElements.empty()) {
        return input;
    }

    TVector<TInfoUnit> newColumns;
    if (!BuildRenamedOutput(originalUnionColumns, pushedRenameSources, pushedElements, &newColumns)) {
        return input;
    }

    for (const auto& child : unionAll->Children) {
        if (!BuildRenamedOutput(child->GetOutputIUs(), pushedRenameSources, pushedElements)) {
            return input;
        }
    }

    ApplyRenamesToResidualExpressions(residualElements, renameMap);
    if (!residualElements.empty() && !ResidualIsValid(newColumns, pushedRenameSources, residualElements)) {
        return input;
    }

    unionAll->Children[0] = MakeIntrusive<TOpMap>(unionAll->Children[0], topMap->Pos, pushedElements, topMap->Ordered);
    unionAll->Children[1] = MakeIntrusive<TOpMap>(unionAll->Children[1], topMap->Pos, pushedElements, topMap->Ordered);
    unionAll->Columns = std::move(newColumns);
    if (!renameMap.empty()) {
        props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);
    }

    if (residualElements.empty()) {
        return unionAll;
    }

    return MakeIntrusive<TOpMap>(unionAll, topMap->Pos, residualElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
