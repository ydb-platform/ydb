#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ x <- a, y <- b ]  == becomes ==>  UnionAll [ x, y ]
// B: `- UnionAll [ a, b ]                       |- Map [ x <- a, y <- b ]
// C:    |- left                                 |  `- left
// D:    `- right                                `- Map [ x <- a, y <- b ]
// E:                                                 `- right
//
// The rule intentionally handles only whole pure semantic-rename Maps. Pushing
// append aliases through UnionAll can move type wrappers below sequence
// extension and change type alignment between branches.

namespace {

bool TryAddColumn(TVector<TInfoUnit>& columns, TInfoUnitSet& columnSet, const TInfoUnit& column) {
    if (columnSet.contains(column)) {
        return false;
    }

    columns.push_back(column);
    columnSet.insert(column);
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

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

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

    const auto unionColumns = unionAll->Columns;
    TInfoUnitSet renameSources;
    for (const auto& mapElement : topMap->MapElements) {
        if (!mapElement.IsRename()) {
            return input;
        }

        const auto from = mapElement.GetRename();
        if (from == mapElement.GetElementName() || !ContainsInfoUnit(unionColumns, from)) {
            return input;
        }

        renameSources.insert(from);
    }

    TVector<TInfoUnit> newColumns;
    if (!BuildRenamedOutput(unionColumns, renameSources, topMap->MapElements, &newColumns)) {
        return input;
    }

    for (const auto& child : unionAll->Children) {
        if (!BuildRenamedOutput(child->GetOutputIUs(), renameSources, topMap->MapElements)) {
            return input;
        }
    }

    unionAll->Children[0] = MakeIntrusive<TOpMap>(unionAll->Children[0], topMap->Pos, topMap->MapElements, topMap->Ordered);
    unionAll->Children[1] = MakeIntrusive<TOpMap>(unionAll->Children[1], topMap->Pos, topMap->MapElements, topMap->Ordered);
    unionAll->Columns = std::move(newColumns);
    return unionAll;
}

} // namespace NKqp
} // namespace NKikimr
