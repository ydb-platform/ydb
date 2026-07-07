#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>

namespace NKikimr {
namespace NKqp {

namespace {

void AddReadColumnByName(const TOpRead& read, const TString& columnName, TInfoUnitSet& requiredColumns) {
    for (const auto& outputIU : read.OutputIUs) {
        if (outputIU.GetFullName() == columnName || outputIU.GetColumnName() == columnName) {
            AddInfoUnit(requiredColumns, outputIU);
        }
    }
}

void AddReadLambdaDeps(const TOpRead& read, const TExprNode::TPtr& lambda, TInfoUnitSet& requiredColumns) {
    const auto inspection = NOpt::InspectOlapProcessLambda(lambda);
    if (inspection.RequiresAllInputColumns) {
        AddInfoUnits(requiredColumns, read.OutputIUs);
        return;
    }

    for (const auto& columnName : inspection.Columns) {
        AddReadColumnByName(read, columnName, requiredColumns);
    }
}

void AddReadExpressionMemberDeps(const TOpRead& read, const TExprNode::TPtr& node, TInfoUnitSet& requiredColumns) {
    if (!node) {
        return;
    }

    if (node->IsCallable("Member")) {
        if (node->ChildrenSize() == 2 && node->Child(1)->IsAtom()) {
            AddReadColumnByName(read, TString(node->Child(1)->Content()), requiredColumns);
        }
        return;
    }

    for (const auto& child : node->ChildrenList()) {
        AddReadExpressionMemberDeps(read, child, requiredColumns);
    }
}

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut, const TInfoUnitSet& keepKeyColumns = {}) {
    TVector<bool> keep(map->MapElements.size(), false);
    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        const auto& mapElement = map->MapElements[idx];
        const auto to = mapElement.GetElementName();
        if (liveOut.contains(to) || keepKeyColumns.contains(to)) {
            keep[idx] = true;
            continue;
        }

        if (mapElement.IsRename() && GetForbidden(map.get()).contains(mapElement.GetRename())) {
            keep[idx] = true;
        }
    }

    bool changed = true;
    while (changed) {
        changed = false;
        TInfoUnitSet keptOutputs;
        for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
            if (keep[idx]) {
                AddInfoUnit(keptOutputs, map->MapElements[idx].GetElementName());
            }
        }

        for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
            const auto& mapElement = map->MapElements[idx];
            if (!keep[idx] && mapElement.IsRename() && keptOutputs.contains(mapElement.GetRename())) {
                keep[idx] = true;
                changed = true;
            }
        }
    }

    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());
    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        if (keep[idx]) {
            const auto& mapElement = map->MapElements[idx];
            newElements.push_back(mapElement);
        }
    }

    return newElements;
}

bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TInfoUnitSet& liveOut, const TInfoUnitSet& keepKeyColumns = {}) {
    TInfoUnitSet requiredColumns;
    for (const auto& outputIU : read->OutputIUs) {
        if (liveOut.contains(outputIU) || keepKeyColumns.contains(outputIU)) {
            AddInfoUnit(requiredColumns, outputIU);
        }
    }

    AddReadLambdaDeps(*read, read->OlapFilterLambda, requiredColumns);
    if (read->OriginalPredicate) {
        AddReadExpressionMemberDeps(*read, read->OriginalPredicate->Node, requiredColumns);
    }

    TVector<TString> newColumns;
    TVector<TInfoUnit> newOutputIUs;
    newColumns.reserve(read->Columns.size());
    newOutputIUs.reserve(read->OutputIUs.size());

    Y_ENSURE(read->Columns.size() == read->OutputIUs.size());
    for (size_t i = 0; i < read->OutputIUs.size(); ++i) {
        if (requiredColumns.contains(read->OutputIUs[i])) {
            newColumns.push_back(read->Columns[i]);
            newOutputIUs.push_back(read->OutputIUs[i]);
        }
    }

    if (newOutputIUs.empty() && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && !read->OutputIUs.empty()) {
        newColumns.push_back(read->Columns.front());
        newOutputIUs.push_back(read->OutputIUs.front());
    }

    if (newOutputIUs == read->OutputIUs) {
        return false;
    }

    read->Columns = std::move(newColumns);
    read->OutputIUs = std::move(newOutputIUs);
    return true;
}

bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TInfoUnitSet& liveOut) {
    TVector<TOpAggregationTraits> newTraits;
    newTraits.reserve(aggregate->AggregationTraitsList.size());
    for (const auto& traits : aggregate->AggregationTraitsList) {
        if (liveOut.contains(traits.ResultColName)) {
            newTraits.push_back(traits);
        }
    }

    if (newTraits.size() == aggregate->AggregationTraitsList.size()) {
        return false;
    }

    aggregate->AggregationTraitsList = std::move(newTraits);
    return true;
}

} // anonymous namespace

bool TPruneDeadMapElementsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map;
}

bool TPruneDeadMapElementsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    const auto& liveOut = GetLiveOut(map.get());

    // If we need to keep key columns, add them to keep list
    TInfoUnitSet keepKeyColumns;
    if (!PruneKeyColumns) {
        for (auto column : input->Props.Metadata->KeyColumns) {
            keepKeyColumns.insert(column);
        }
    }

    auto newElements = KeepLiveMapElements(map, liveOut, keepKeyColumns);

    if (newElements.empty()) {
        input = map->GetInput();
    } else {
        if (newElements.size() == map->MapElements.size()) {
            return false;
        }

        map->MapElements = std::move(newElements);
    }

    return true;
}

bool TPruneDeadReadColumnsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Source;
}

bool TPruneDeadReadColumnsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Source) {
        return false;
    }

    auto read = CastOperator<TOpRead>(input);
    const auto& liveOut = GetLiveOut(read.get());

    // If we need to keep key columns, add them to keep list
    TInfoUnitSet keepKeyColumns;
    if (!PruneKeyColumns) {
        for (auto column : input->Props.Metadata->KeyColumns) {
            keepKeyColumns.insert(column);
        }
    }

    return NarrowReadColumns(read, liveOut, keepKeyColumns);
}

bool TPruneDeadUnionAllColumnsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::UnionAll;
}

bool TPruneDeadUnionAllColumnsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::UnionAll) {
        return false;
    }

    auto unionAll = CastOperator<TOpUnionAll>(input);
    const auto& liveOut = GetLiveOut(unionAll.get());

    TVector<TInfoUnit> newColumns;
    newColumns.reserve(unionAll->Columns.size());
    for (const auto& column : unionAll->Columns) {
        if (liveOut.contains(column)) {
            newColumns.push_back(column);
        }
    }

    if (newColumns.size() == unionAll->Columns.size()) {
        return false;
    }

    // Keep at least one column; TOpUnionAll::PropagateLiveness keeps the same
    // column alive in the branches.
    if (newColumns.empty() && !unionAll->Columns.empty()) {
        newColumns.push_back(unionAll->Columns.front());
        if (newColumns.size() == unionAll->Columns.size()) {
            return false;
        }
    }

    unionAll->Columns = std::move(newColumns);
    return true;
}

bool TPruneDeadAggregateTraitsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Aggregate;
}

bool TPruneDeadAggregateTraitsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Aggregate || CastOperator<TOpAggregate>(input)->IsDistinctAll()) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(input);
    const auto& liveOut = GetLiveOut(aggregate.get());

    return PruneAggregateTraits(aggregate, liveOut);
}

} // namespace NKqp
} // namespace NKikimr
