#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

using TInfoUnitSet = THashSet<TInfoUnit, TInfoUnit::THashFunction>;

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

TInfoUnitSet MakeSet(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    AddColumnsToSet(result, ius);
    return result;
}

class TLogicalLiveness {
public:
    explicit TLogicalLiveness(TPlanProps& props)
        : Props(props) {
    }

    void Run(TOpRoot& root) {
        TVector<TInfoUnit> rootColumns;
        rootColumns.reserve(root.ColumnOrder.size());
        for (const auto& column : root.ColumnOrder) {
            rootColumns.emplace_back(column);
        }

        AddLiveColumns(root.GetInput(), rootColumns);
        Propagate();
        PruneDeadMapElements(root);
    }

private:
    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& columns) {
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

    bool AddLiveColumns(const TIntrusivePtr<IOperator>& op, const TInfoUnitSet& columns) {
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

    void Enqueue(const TIntrusivePtr<IOperator>& op) {
        if (Queued.insert(op.get()).second) {
            Queue.push_back(op);
        }
    }

    void Propagate() {
        for (size_t index = 0; index < Queue.size(); ++index) {
            auto op = Queue[index];
            Queued.erase(op.get());
            PropagateOperator(op);
        }
        Queue.clear();
    }

    void AddExpressionDeps(const TExpression& expr, TInfoUnitSet& target) {
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

    void AddPassthroughColumns(const TIntrusivePtr<IOperator>& input, const TInfoUnitSet& liveOut) {
        TInfoUnitSet inputLive;
        for (const auto& iu : input->GetOutputIUs()) {
            if (liveOut.contains(iu)) {
                AddLiveColumn(inputLive, iu);
            }
        }
        AddLiveColumns(input, inputLive);
    }

    void PropagateUnaryPassthrough(const TIntrusivePtr<IUnaryOperator>& op) {
        AddPassthroughColumns(op->GetInput(), LiveOut[op.get()]);
    }

    void PropagateMap(const TIntrusivePtr<TOpMap>& map) {
        const auto& liveOut = LiveOut[map.get()];
        auto input = map->GetInput();
        TInfoUnitSet inputLive;
        TInfoUnitSet renameSources;

        for (const auto& mapElement : map->MapElements) {
            if (mapElement.IsRename()) {
                renameSources.insert(mapElement.GetRename());
                // Renames are not pruned in this stage, so their source must stay available.
                AddExpressionDeps(mapElement.GetExpression(), inputLive);
            }
        }

        for (const auto& iu : input->GetOutputIUs()) {
            if (!renameSources.contains(iu) && liveOut.contains(iu)) {
                AddLiveColumn(inputLive, iu);
            }
        }

        for (const auto& mapElement : map->MapElements) {
            if (!mapElement.IsRename() && liveOut.contains(mapElement.GetElementName())) {
                AddExpressionDeps(mapElement.GetExpression(), inputLive);
            }
        }

        AddLiveColumns(input, inputLive);
    }

    void PropagateFilter(const TIntrusivePtr<TOpFilter>& filter) {
        TInfoUnitSet inputLive = LiveOut[filter.get()];
        AddExpressionDeps(filter->FilterExpr, inputLive);
        AddLiveColumns(filter->GetInput(), inputLive);
    }

    void PropagateAddDependencies(const TIntrusivePtr<TOpAddDependencies>& deps) {
        AddPassthroughColumns(deps->GetInput(), LiveOut[deps.get()]);
    }

    void PropagateLimit(const TIntrusivePtr<TOpLimit>& limit) {
        TInfoUnitSet inputLive = LiveOut[limit.get()];
        AddExpressionDeps(limit->GetLimitCond(), inputLive);
        if (const auto offset = limit->GetOffsetCond()) {
            AddExpressionDeps(*offset, inputLive);
        }
        AddLiveColumns(limit->GetInput(), inputLive);
    }

    void PropagateSort(const TIntrusivePtr<TOpSort>& sort) {
        TInfoUnitSet inputLive = LiveOut[sort.get()];
        for (const auto& sortElement : sort->GetSortElements()) {
            AddLiveColumn(inputLive, sortElement.SortColumn);
        }
        if (sort->LimitCond) {
            AddExpressionDeps(*sort->LimitCond, inputLive);
        }
        AddLiveColumns(sort->GetInput(), inputLive);
    }

    void PropagateAggregate(const TIntrusivePtr<TOpAggregate>& aggregate) {
        TInfoUnitSet inputLive;
        AddColumnsToSet(inputLive, aggregate->GetKeyColumns());
        for (const auto& traits : aggregate->GetAggregationTraits()) {
            AddLiveColumn(inputLive, traits.OriginalColName);
        }
        AddLiveColumns(aggregate->GetInput(), inputLive);
    }

    void PropagateJoin(const TIntrusivePtr<TOpJoin>& join) {
        const auto& liveOut = LiveOut[join.get()];
        const auto leftInput = join->GetLeftInput();
        const auto rightInput = join->GetRightInput();
        const auto leftOutput = MakeSet(leftInput->GetOutputIUs());
        const auto rightOutput = MakeSet(rightInput->GetOutputIUs());

        TInfoUnitSet leftLive;
        TInfoUnitSet rightLive;

        const bool outputsLeft = join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";
        const bool outputsRight = join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";

        if (outputsLeft) {
            for (const auto& iu : leftOutput) {
                if (liveOut.contains(iu)) {
                    AddLiveColumn(leftLive, iu);
                }
            }
        }

        if (outputsRight) {
            for (const auto& iu : rightOutput) {
                if (liveOut.contains(iu)) {
                    AddLiveColumn(rightLive, iu);
                }
            }
        }

        for (const auto& [leftKey, rightKey] : join->JoinKeys) {
            AddLiveColumn(leftLive, leftKey);
            AddLiveColumn(rightLive, rightKey);
        }

        for (const auto& filter : join->JoinFilters) {
            TInfoUnitSet filterDeps;
            AddExpressionDeps(filter, filterDeps);
            for (const auto& iu : filterDeps) {
                if (leftOutput.contains(iu)) {
                    AddLiveColumn(leftLive, iu);
                }
                if (rightOutput.contains(iu)) {
                    AddLiveColumn(rightLive, iu);
                }
            }
        }

        AddLiveColumns(leftInput, leftLive);
        AddLiveColumns(rightInput, rightLive);
    }

    void PropagateUnionAll(const TIntrusivePtr<TOpUnionAll>& unionAll) {
        const auto& liveOut = LiveOut[unionAll.get()];
        AddLiveColumns(unionAll->GetLeftInput(), liveOut);
        AddLiveColumns(unionAll->GetRightInput(), liveOut);
    }

    void PropagateCBOTree(const TIntrusivePtr<TOpCBOTree>& cboTree) {
        for (const auto& child : cboTree->Children) {
            AddLiveColumns(child, child->GetOutputIUs());
        }
    }

    void PropagateOperator(const TIntrusivePtr<IOperator>& op) {
        switch (op->Kind) {
            case EOperator::EmptySource:
            case EOperator::Source:
                return;
            case EOperator::Map:
                return PropagateMap(CastOperator<TOpMap>(op));
            case EOperator::AddDependencies:
                return PropagateAddDependencies(CastOperator<TOpAddDependencies>(op));
            case EOperator::Filter:
                return PropagateFilter(CastOperator<TOpFilter>(op));
            case EOperator::Join:
                return PropagateJoin(CastOperator<TOpJoin>(op));
            case EOperator::Aggregate:
                return PropagateAggregate(CastOperator<TOpAggregate>(op));
            case EOperator::Limit:
                return PropagateLimit(CastOperator<TOpLimit>(op));
            case EOperator::Sort:
                return PropagateSort(CastOperator<TOpSort>(op));
            case EOperator::UnionAll:
                return PropagateUnionAll(CastOperator<TOpUnionAll>(op));
            case EOperator::CBOTree:
                return PropagateCBOTree(CastOperator<TOpCBOTree>(op));
            case EOperator::Root:
                return PropagateUnaryPassthrough(CastOperator<IUnaryOperator>(op));
        }
    }

    bool PruneDeadMapElements(TOpRoot& root) {
        bool changed = false;
        for (auto it : root) {
            if (it.Current->Kind != EOperator::Map) {
                continue;
            }

            auto map = CastOperator<TOpMap>(it.Current);
            const auto liveIt = LiveOut.find(map.get());
            if (liveIt == LiveOut.end()) {
                continue;
            }

            const auto& liveOut = liveIt->second;
            TVector<TMapElement> newElements;
            newElements.reserve(map->MapElements.size());
            for (const auto& mapElement : map->MapElements) {
                if (mapElement.IsRename() || liveOut.contains(mapElement.GetElementName())) {
                    newElements.push_back(mapElement);
                }
            }

            if (newElements.size() != map->MapElements.size()) {
                map->MapElements = std::move(newElements);
                changed = true;
            }
        }

        while (RemoveOneEmptyMap(root)) {
            changed = true;
            root.ComputeParents();
        }

        if (changed) {
            root.ComputeParents();
        }

        return changed;
    }

    bool RemoveOneEmptyMap(TOpRoot& root) {
        root.ComputeParents();
        for (auto it : root) {
            if (it.Current->Kind != EOperator::Map) {
                continue;
            }

            auto map = CastOperator<TOpMap>(it.Current);
            if (!map->MapElements.empty()) {
                continue;
            }

            auto replacement = map->GetInput();
            if (it.Current->Parents.empty()) {
                if (it.SubplanIU) {
                    Props.Subplans.Replace(*it.SubplanIU, replacement);
                } else {
                    root.SetInput(replacement);
                }
            } else {
                for (auto& [parent, childIdx] : it.Current->Parents) {
                    parent->Children[childIdx] = replacement;
                }
            }
            return true;
        }
        return false;
    }

private:
    TPlanProps& Props;
    THashMap<IOperator*, TInfoUnitSet> LiveOut;
    THashSet<IOperator*> Queued;
    TVector<TIntrusivePtr<IOperator>> Queue;
};

} // anonymous namespace

TLogicalLivenessStage::TLogicalLivenessStage()
    : IRBOStage("Logical liveness") {
    Props = ERuleProperties::RequireParents;
}

void TLogicalLivenessStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);
    TLogicalLiveness(root.PlanProps).Run(root);
}

} // namespace NKqp
} // namespace NKikimr
