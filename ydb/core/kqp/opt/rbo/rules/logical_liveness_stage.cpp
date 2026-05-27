#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

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

class TLogicalLiveness: public ILivenessContext {
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
