#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/physical/kqp_olap_filter_inspection.h>

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql::NNodes;

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

TInfoUnitSet MakeInfoUnitSetLocal(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    AddColumnsToSet(result, ius);
    return result;
}

class TLogicalLiveness: public ILivenessContext {
public:
    explicit TLogicalLiveness(TPlanProps& props)
        : Props(props)
        , LiveOut(props.LiveOut) {
    }

    void Run(TOpRoot& root) {
        LiveOut.clear();

        TVector<TInfoUnit> rootColumns;
        rootColumns.reserve(root.ColumnOrder.size());
        for (const auto& column : root.ColumnOrder) {
            rootColumns.emplace_back(column);
        }

        AddLiveColumns(root.GetInput(), rootColumns);
        Propagate();
    }

    const TInfoUnitSet& GetLiveOut(IOperator* op) const override {
        const auto it = LiveOut.find(op);
        Y_ENSURE(it != LiveOut.end(), "Liveness requested for an operator that has no live output");
        return it->second;
    }

    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& columns) override {
        bool changed = false;
        auto& live = LiveOut[op.get()];
        for (const auto& column : columns) {
            changed |= AddLiveColumn(live, column);
        }
        if (changed) {
            Enqueue(op);
        }
        return changed;
    }

    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TInfoUnitSet& columns) override {
        bool changed = false;
        auto& live = LiveOut[op.get()];
        for (const auto& column : columns) {
            changed |= AddLiveColumn(live, column);
        }
        if (changed) {
            Enqueue(op);
        }
        return changed;
    }

    void AddExpressionDeps(const TExpression& expr, TInfoUnitSet& target) override {
        AddColumnsToSet(target, expr.GetInputIUs(false, true));

        for (const auto& iu : expr.GetInputIUs(true, false)) {
            if (!iu.IsSubplanContext()) {
                continue;
            }

            const auto it = Props.Subplans.PlanMap.find(iu);
            if (it == Props.Subplans.PlanMap.end()) {
                continue;
            }

            AddLiveColumns(CastOperator<IOperator>(it->second.Plan), CastOperator<IOperator>(it->second.Plan)->GetOutputIUs());
        }
    }

private:
    void Enqueue(const TIntrusivePtr<IOperator>& op) {
        if (Queued.insert(op.get()).second) {
            Queue.push_back(op);
        }
    }

    void Propagate() {
        for (size_t index = 0; index < Queue.size(); ++index) {
            auto op = Queue[index];
            Queued.erase(op.get());
            op->PropagateLiveness(*this);
        }
        Queue.clear();
    }

    TPlanProps& Props;
    THashMap<IOperator*, TInfoUnitSet>& LiveOut;
    THashSet<IOperator*> Queued;
    TVector<TIntrusivePtr<IOperator>> Queue;
};

class TLogicalNameConstraints {
public:
    explicit TLogicalNameConstraints(TPlanProps& props)
        : Props(props) {
    }

    void Run(TOpRoot& root) {
        Props.NameConstraints.Clear();

        bool changed = true;
        ui32 iteration = 0;
        while (changed) {
            changed = false;
            for (const auto& iter : root) {
                changed |= Transfer(iter.Current);
            }
            Y_ENSURE(++iteration < 1000, "Name constraint propagation did not converge");
        }
    }

private:
    TInfoUnitSet GetIncomingForbidden(const TIntrusivePtr<IOperator>& op) const {
        TInfoUnitSet result;
        for (const auto& [parent, childIdx] : op->Parents) {
            AddColumnsToSet(result, Props.NameConstraints.GetForbiddenOut(parent, childIdx, op.get()));
        }
        return result;
    }

    bool AddForbiddenToChild(const TIntrusivePtr<IOperator>& parent, ui32 childIdx, const TInfoUnitSet& forbidden) {
        if (forbidden.empty()) {
            return false;
        }
        Y_ENSURE(childIdx < parent->Children.size());
        return Props.NameConstraints.AddForbiddenOut(parent.get(), childIdx, parent->Children[childIdx].get(), forbidden);
    }

    bool TransferPassthroughUnary(const TIntrusivePtr<IUnaryOperator>& op) {
        const auto incoming = GetIncomingForbidden(op);
        const auto inputOutput = MakeInfoUnitSetLocal(op->GetInput()->GetOutputIUs());

        TInfoUnitSet childForbidden;
        for (const auto& iu : incoming) {
            if (inputOutput.contains(iu)) {
                AddLiveColumn(childForbidden, iu);
            }
        }

        return AddForbiddenToChild(op, 0, childForbidden);
    }

    bool TransferMap(const TIntrusivePtr<TOpMap>& map) {
        const auto incoming = GetIncomingForbidden(map);
        const auto inputOutput = MakeInfoUnitSetLocal(map->GetInput()->GetOutputIUs());

        TInfoUnitSet renameSources;
        for (const auto& mapElement : map->MapElements) {
            if (mapElement.IsRename()) {
                AddLiveColumn(renameSources, mapElement.GetRename());
            }
        }

        TInfoUnitSet childForbidden;
        for (const auto& iu : incoming) {
            if (inputOutput.contains(iu) && !renameSources.contains(iu)) {
                AddLiveColumn(childForbidden, iu);
            }
        }

        return AddForbiddenToChild(map, 0, childForbidden);
    }

    bool TransferAddDependencies(const TIntrusivePtr<TOpAddDependencies>& deps) {
        const auto incoming = GetIncomingForbidden(deps);
        const auto inputOutput = MakeInfoUnitSetLocal(deps->GetInput()->GetOutputIUs());

        TInfoUnitSet childForbidden;
        for (const auto& iu : incoming) {
            if (inputOutput.contains(iu)) {
                AddLiveColumn(childForbidden, iu);
            }
        }

        return AddForbiddenToChild(deps, 0, childForbidden);
    }

    bool TransferJoin(const TIntrusivePtr<TOpJoin>& join) {
        const bool outputsLeft = join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";
        const bool outputsRight = join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";

        const auto incoming = GetIncomingForbidden(join);
        const auto leftOutput = join->GetLeftInput()->GetOutputIUs();
        const auto rightOutput = join->GetRightInput()->GetOutputIUs();
        const auto leftOutputSet = MakeInfoUnitSetLocal(leftOutput);
        const auto rightOutputSet = MakeInfoUnitSetLocal(rightOutput);

        TInfoUnitSet leftForbidden;
        TInfoUnitSet rightForbidden;

        if (outputsLeft && outputsRight) {
            AddColumnsToSet(leftForbidden, rightOutput);
            AddColumnsToSet(rightForbidden, leftOutput);
        }

        for (const auto& iu : incoming) {
            if (outputsLeft && leftOutputSet.contains(iu)) {
                AddLiveColumn(leftForbidden, iu);
            }
            if (outputsRight && rightOutputSet.contains(iu)) {
                AddLiveColumn(rightForbidden, iu);
            }
        }

        bool changed = false;
        changed |= AddForbiddenToChild(join, 0, leftForbidden);
        changed |= AddForbiddenToChild(join, 1, rightForbidden);
        return changed;
    }

    bool TransferUnionAll(const TIntrusivePtr<TOpUnionAll>& unionAll) {
        const auto incoming = GetIncomingForbidden(unionAll);
        const auto schema = MakeInfoUnitSetLocal(unionAll->GetOutputIUs());

        bool changed = false;
        for (ui32 childIdx = 0; childIdx < unionAll->Children.size(); ++childIdx) {
            const auto& child = unionAll->Children[childIdx];
            const auto childOutput = child->GetOutputIUs();
            const auto childOutputSet = MakeInfoUnitSetLocal(childOutput);

            TInfoUnitSet childForbidden;
            for (const auto& iu : childOutput) {
                if (!schema.contains(iu)) {
                    AddLiveColumn(childForbidden, iu);
                }
            }
            for (const auto& iu : incoming) {
                if (childOutputSet.contains(iu)) {
                    AddLiveColumn(childForbidden, iu);
                }
            }

            changed |= AddForbiddenToChild(unionAll, childIdx, childForbidden);
        }

        return changed;
    }

    bool Transfer(const TIntrusivePtr<IOperator>& op) {
        switch (op->Kind) {
            case EOperator::Filter:
            case EOperator::Limit:
            case EOperator::Sort:
                return TransferPassthroughUnary(CastOperator<IUnaryOperator>(op));
            case EOperator::Map:
                return TransferMap(CastOperator<TOpMap>(op));
            case EOperator::AddDependencies:
                return TransferAddDependencies(CastOperator<TOpAddDependencies>(op));
            case EOperator::Join:
                return TransferJoin(CastOperator<TOpJoin>(op));
            case EOperator::UnionAll:
                return TransferUnionAll(CastOperator<TOpUnionAll>(op));
            default:
                return false;
        }
    }

    TPlanProps& Props;
};

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

void AddReadColumnByName(const TOpRead& read, const TString& columnName, TInfoUnitSet& requiredColumns) {
    for (const auto& outputIU : read.OutputIUs) {
        if (outputIU.GetFullName() == columnName || outputIU.GetColumnName() == columnName) {
            AddLiveColumn(requiredColumns, outputIU);
        }
    }
}

void AddReadLambdaDeps(const TOpRead& read, const TExprNode::TPtr& lambda, TInfoUnitSet& requiredColumns) {
    const auto inspection = NOpt::InspectOlapProcessLambda(lambda);
    if (inspection.RequiresAllInputColumns) {
        AddColumnsToSet(requiredColumns, read.OutputIUs);
        return;
    }

    for (const auto& columnName : inspection.Columns) {
        AddReadColumnByName(read, columnName, requiredColumns);
    }
}

bool NarrowReadColumns(const TIntrusivePtr<TOpRead>& read, const TVector<TInfoUnit>& liveOutput) {
    TInfoUnitSet requiredColumns;
    AddColumnsToSet(requiredColumns, liveOutput);
    AddReadLambdaDeps(*read, read->OlapFilterLambda, requiredColumns);

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

} // anonymous namespace

void ComputePlanLiveness(TOpRoot& root) {
    TLogicalLiveness(root.PlanProps).Run(root);
}

void ComputePlanNameConstraints(TOpRoot& root) {
    TLogicalNameConstraints(root.PlanProps).Run(root);
}

bool TPruneDeadMapElementsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    const auto liveIt = props.LiveOut.find(map.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    auto newElements = KeepLiveMapElements(map, liveIt->second, props);
    if (newElements.size() == map->MapElements.size()) {
        return false;
    }

    if (newElements.empty()) {
        if (!CanReplaceInParents(map, map->GetInput(), props)) {
            return false;
        }
        input = map->GetInput();
    } else {
        auto oldElements = std::move(map->MapElements);
        map->MapElements = std::move(newElements);
        if (!CanExposeToParents(map.get(), props)) {
            map->MapElements = std::move(oldElements);
            return false;
        }
    }

    return true;
}

bool TPruneDeadReadColumnsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Source) {
        return false;
    }

    auto read = CastOperator<TOpRead>(input);
    const auto liveIt = props.LiveOut.find(read.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    const auto liveOutput = KeepLiveColumns(read->GetOutputIUs(), liveIt->second);
    return NarrowReadColumns(read, liveOutput);
}

bool TPruneDeadAggregateTraitsRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Aggregate) {
        return false;
    }

    auto aggregate = CastOperator<TOpAggregate>(input);
    const auto liveIt = props.LiveOut.find(aggregate.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveIt->second);
    return PruneAggregateTraits(aggregate, liveOutput);
}

TNarrowByLivenessStage::TNarrowByLivenessStage()
    : IRBOStage("Narrow by liveness") {
    Props = ERuleProperties::RequireParents;
}

void TNarrowByLivenessStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);

    for (const auto& iter : root) {
        iter.Current->ClearOutputIUsOverride();
    }

    bool pruned = true;
    while (pruned) {
        pruned = false;
        ComputePlanLiveness(root);
        ComputePlanNameConstraints(root);

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Map) {
                continue;
            }

            auto map = CastOperator<TOpMap>(iter.Current);
            const auto liveIt = root.PlanProps.LiveOut.find(map.get());
            if (liveIt == root.PlanProps.LiveOut.end()) {
                continue;
            }

            auto newElements = KeepLiveMapElements(map, liveIt->second, root.PlanProps);
            if (newElements.size() == map->MapElements.size()) {
                continue;
            }

            auto oldElements = std::move(map->MapElements);
            map->MapElements = std::move(newElements);
            if (!CanExposeToParents(map.get(), root.PlanProps)) {
                map->MapElements = std::move(oldElements);
                continue;
            }

            pruned = true;
        }

        for (const auto& iter : root) {
            if (iter.Current->Kind != EOperator::Aggregate) {
                continue;
            }

            auto aggregate = CastOperator<TOpAggregate>(iter.Current);
            const auto liveIt = root.PlanProps.LiveOut.find(aggregate.get());
            if (liveIt == root.PlanProps.LiveOut.end()) {
                continue;
            }

            const auto liveOutput = KeepLiveColumns(aggregate->GetOutputIUs(), liveIt->second);
            pruned |= PruneAggregateTraits(aggregate, liveOutput);
        }
    }

    ComputePlanLiveness(root);

    THashMap<IOperator*, TVector<TInfoUnit>> liveOutputs;
    for (const auto& iter : root) {
        const auto liveIt = root.PlanProps.LiveOut.find(iter.Current.get());
        if (liveIt == root.PlanProps.LiveOut.end()) {
            continue;
        }

        auto liveOut = liveIt->second;
        if (iter.Current->Kind == EOperator::Sort) {
            for (const auto& sortElement : CastOperator<TOpSort>(iter.Current)->SortElements) {
                AddLiveColumn(liveOut, sortElement.SortColumn);
            }
        }

        liveOutputs[iter.Current.get()] = KeepLiveColumns(iter.Current->GetOutputIUs(), liveOut);
    }

    for (const auto& iter : root) {
        auto op = iter.Current;
        const auto outputIt = liveOutputs.find(op.get());
        if (outputIt == liveOutputs.end()) {
            continue;
        }

        const auto& liveOutput = outputIt->second;
        if (op->Kind == EOperator::Source) {
            NarrowReadColumns(CastOperator<TOpRead>(op), liveOutput);
            continue;
        }

        if (op->Kind == EOperator::Aggregate) {
            PruneAggregateTraits(CastOperator<TOpAggregate>(op), liveOutput);
        }

        if (!CanOverrideOutput(op->Kind)) {
            continue;
        }

        if (op->GetOutputIUs() != liveOutput) {
            op->SetOutputIUsOverride(liveOutput);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
