#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

#include <ydb/core/kqp/opt/physical/kqp_olap_filter_inspection.h>

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

void AddReadOriginalPredicateDeps(const TOpRead& read, TInfoUnitSet& requiredColumns) {
    if (!read.OriginalPredicate) {
        return;
    }

    AddReadExpressionMemberDeps(read, read.OriginalPredicate->Node, requiredColumns);
}

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut, const TInfoUnitSet& keepKeyColumns = {}) {
    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());

    auto isShadowedByKeptOutput = [&](const TInfoUnit& source) {
        for (const auto& mapElement : map->MapElements) {
            const auto output = mapElement.GetElementName();
            if (output == source && (liveOut.contains(output) || keepKeyColumns.contains(output))) {
                return true;
            }
        }
        return false;
    };

    for (const auto& mapElement : map->MapElements) {
        const auto to = mapElement.GetElementName();
        if (mapElement.IsRename()) {
            const auto from = mapElement.GetRename();
            if (liveOut.contains(to) || keepKeyColumns.contains(to) ||
                GetForbidden(map.get()).contains(from) ||
                isShadowedByKeptOutput(from))
            {
                newElements.push_back(mapElement);
            }
        } else if (liveOut.contains(to) || keepKeyColumns.contains(to)) {
            newElements.push_back(mapElement);
        }
    }

    return newElements;
}

TVector<TInfoUnit> KeepLiveColumns(const TVector<TInfoUnit>& columns, const TInfoUnitSet& liveOut, const TInfoUnitSet& keepKeyColumns = {}) {
    TVector<TInfoUnit> newColumns;
    newColumns.reserve(columns.size());

    for (const auto& column : columns) {
        if (liveOut.contains(column) || keepKeyColumns.contains(column)) {
            newColumns.push_back(column);
        }
    }

    return newColumns;
}

bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet requiredColumns;
    AddInfoUnits(requiredColumns, liveOutput);
    AddReadLambdaDeps(*read, read->OlapFilterLambda, requiredColumns);
    AddReadOriginalPredicateDeps(*read, requiredColumns);

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
    read->Props.OutputIUs = read->OutputIUs;
    return true;
}

bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet liveOutputSet;
    AddInfoUnits(liveOutputSet, liveOutput);

    TVector<TOpAggregationTraits> newTraits;
    newTraits.reserve(aggregate->AggregationTraitsList.size());
    for (const auto& traits : aggregate->AggregationTraitsList) {
        if (liveOutputSet.contains(traits.ResultColName)) {
            newTraits.push_back(traits);
        }
    }

    if (newTraits.size() == aggregate->AggregationTraitsList.size()) {
        return false;
    }

    aggregate->AggregationTraitsList = std::move(newTraits);
    TVector<TInfoUnit> output = aggregate->KeyColumns;
    for (const auto& traits : aggregate->AggregationTraitsList) {
        output.push_back(traits.ResultColName);
    }
    aggregate->Props.OutputIUs = std::move(output);
    return true;
}

} // anonymous namespace

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
        const auto& replacementOutput = map->GetInput()->GetOutputIUs();
        if (MakeInfoUnitSet(replacementOutput).size() != replacementOutput.size() ||
            !IUSetIntersect(replacementOutput, GetForbidden(map.get())).empty()) {
            return false;
        }
        input = map->GetInput();
    } else {
        if (newElements.size() == map->MapElements.size()) {
            return false;
        }

        auto newOutput = BuildMapOutput(map, newElements);
        if (MakeInfoUnitSet(newOutput).size() != newOutput.size() ||
            !IUSetIntersect(newOutput, GetForbidden(map.get())).empty()) {
            return false;
        }
        map->MapElements = std::move(newElements);
        map->Props.OutputIUs = std::move(newOutput);
    }

    return true;
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

    const auto liveOutput = KeepLiveColumns(read->GetOutputIUs(), liveOut, keepKeyColumns);
    return NarrowReadColumns(read, liveOutput);
}

bool TPruneDeadAggregateTraitsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Aggregate || CastOperator<TOpAggregate>(input)->IsDistinctAll()) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(input);
    const auto& liveOut = GetLiveOut(aggregate.get());

    // Key columns will be preserved in the aggregate anyway
    const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveOut);
    return PruneAggregateTraits(aggregate, liveOutput);
}

} // namespace NKqp
} // namespace NKikimr
