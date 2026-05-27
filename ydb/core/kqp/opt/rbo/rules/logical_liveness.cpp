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

TVector<TMapElement> KeepLiveMapElements(const TIntrusivePtr<TOpMap>& map, const TInfoUnitSet& liveOut) {
    TVector<TMapElement> newElements;
    newElements.reserve(map->MapElements.size());

    for (const auto& mapElement : map->MapElements) {
        if (mapElement.IsRename() || liveOut.contains(mapElement.GetElementName())) {
            newElements.push_back(mapElement);
        }
    }

    return newElements;
}

} // anonymous namespace

void ComputePlanLiveness(TOpRoot& root) {
    TLogicalLiveness(root.PlanProps).Run(root);
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

    auto newElements = KeepLiveMapElements(map, liveIt->second);
    if (newElements.size() == map->MapElements.size()) {
        return false;
    }

    if (newElements.empty()) {
        input = map->GetInput();
    } else {
        map->MapElements = std::move(newElements);
    }

    return true;
}

} // namespace NKqp
} // namespace NKikimr
