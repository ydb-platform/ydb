#include "logical_liveness_helpers.h"

#include <ydb/core/kqp/opt/physical/kqp_olap_filter_inspection.h>

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql::NNodes;

void AddReadColumnByName(const TOpRead& read, const TString& columnName, TInfoUnitSet& requiredColumns) {
    for (const auto& outputIU : read.OutputIUs) {
        if (outputIU.GetFullName() == columnName || outputIU.GetColumnName() == columnName) {
            AddLiveColumn(requiredColumns, outputIU);
        }
    }
}

const TStructExprType* GetStructItemType(const TTypeAnnotationNode* type) {
    if (!type) {
        return nullptr;
    }

    switch (type->GetKind()) {
        case ETypeAnnotationKind::Struct:
            return type->Cast<TStructExprType>();
        case ETypeAnnotationKind::List:
            return GetStructItemType(type->Cast<TListExprType>()->GetItemType());
        case ETypeAnnotationKind::Flow:
            return GetStructItemType(type->Cast<TFlowExprType>()->GetItemType());
        default:
            return nullptr;
    }
}

void AddReadLambdaInputTypeDeps(const TOpRead& read, const TExprNode::TPtr& lambda, TInfoUnitSet& requiredColumns) {
    if (!lambda || !lambda->IsLambda() || lambda->Head().ChildrenSize() == 0) {
        return;
    }

    const auto* structType = GetStructItemType(lambda->Head().Child(0)->GetTypeAnn());
    if (!structType) {
        return;
    }

    for (const auto* item : structType->GetItems()) {
        AddReadColumnByName(read, TString(item->GetName()), requiredColumns);
    }
}

void AddReadLambdaDeps(const TOpRead& read, const TExprNode::TPtr& lambda, TInfoUnitSet& requiredColumns) {
    AddReadLambdaInputTypeDeps(read, lambda, requiredColumns);

    const auto inspection = NOpt::InspectOlapProcessLambda(lambda);
    if (inspection.RequiresAllInputColumns) {
        AddColumnsToSet(requiredColumns, read.OutputIUs);
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

} // anonymous namespace

bool AddLiveColumn(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

bool AddColumnsToSet(TInfoUnitSet& target, const TVector<TInfoUnit>& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddLiveColumn(target, iu);
    }
    return changed;
}

bool AddColumnsToSet(TInfoUnitSet& target, const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddLiveColumn(target, iu);
    }
    return changed;
}

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut, const TPlanProps& props) {
    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());

    for (const auto& mapElement : map->MapElements) {
        const auto to = mapElement.GetElementName();
        if (mapElement.IsRename()) {
            const auto from = mapElement.GetRename();
            if (liveOut.contains(to) || props.NameConstraints.IsForbiddenAtOutput(map.get(), from)) {
                newElements.push_back(mapElement);
            }
        } else if (liveOut.contains(to)) {
            newElements.push_back(mapElement);
        }
    }

    return newElements;
}

TVector<TInfoUnit> KeepLiveColumns(const TVector<TInfoUnit>& columns, const TInfoUnitSet& liveOut) {
    TVector<TInfoUnit> newColumns;
    newColumns.reserve(columns.size());

    for (const auto& column : columns) {
        if (liveOut.contains(column)) {
            newColumns.push_back(column);
        }
    }

    return newColumns;
}

bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput) {
    if (read->OlapFilterLambda) {
        return false;
    }

    TInfoUnitSet requiredColumns;
    AddColumnsToSet(requiredColumns, liveOutput);
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
    return true;
}

bool PruneAggregateTraits(const TIntrusivePtr<TOpAggregate>& aggregate, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet liveOutputSet;
    AddColumnsToSet(liveOutputSet, liveOutput);

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
    return true;
}

bool CanOverrideOutput(EOperator kind) {
    switch (kind) {
        case EOperator::Map:
        case EOperator::Filter:
        case EOperator::Join:
        case EOperator::Aggregate:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::UnionAll:
            return true;
        default:
            return false;
    }
}

} // namespace NKqp
} // namespace NKikimr
