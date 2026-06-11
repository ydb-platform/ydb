#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>

namespace NKikimr {
namespace NKqp {

namespace {

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
            changed |= AddInfoUnit(live, column);
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
            changed |= AddInfoUnit(live, column);
        }
        if (changed) {
            Enqueue(op);
        }
        return changed;
    }

    void AddExpressionDeps(const TExpression& expr, TInfoUnitSet& target) override {
        AddInfoUnits(target, expr.GetInputIUs(false, true));

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

} // anonymous namespace

void IOperator::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

void IUnaryOperator::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    TInfoUnitSet inputLive;
    for (const auto& iu : GetInput()->GetOutputIUs()) {
        if (liveOut.contains(iu)) {
            AddInfoUnit(inputLive, iu);
        }
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpRead::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

void TOpMap::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    auto input = GetInput();
    TInfoUnitSet inputLive;
    TInfoUnitSet renameSources;

    for (const auto& mapElement : MapElements) {
        if (mapElement.IsRename()) {
            renameSources.insert(mapElement.GetRename());
        }
        // Keep dependencies of every current map expression live so local pruning
        // cannot remove producer columns before the dead consumer expression is gone.
        ctx.AddExpressionDeps(mapElement.GetExpression(), inputLive);
    }

    for (const auto& iu : input->GetOutputIUs()) {
        if (!renameSources.contains(iu) && liveOut.contains(iu)) {
            AddInfoUnit(inputLive, iu);
        }
    }

    ctx.AddLiveColumns(input, inputLive);
}

void TOpFilter::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(FilterExpr, inputLive);
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpJoin::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    const auto leftInput = GetLeftInput();
    const auto rightInput = GetRightInput();
    const auto leftOutput = MakeInfoUnitSet(leftInput->GetOutputIUs());
    const auto rightOutput = MakeInfoUnitSet(rightInput->GetOutputIUs());

    TInfoUnitSet leftLive;
    TInfoUnitSet rightLive;

    const bool outputsLeft = JoinKind != "RightOnly" && JoinKind != "RightSemi";
    const bool outputsRight = JoinKind != "LeftOnly" && JoinKind != "LeftSemi";

    if (outputsLeft) {
        for (const auto& iu : leftOutput) {
            if (liveOut.contains(iu)) {
                AddInfoUnit(leftLive, iu);
            }
        }
    }

    if (outputsRight) {
        for (const auto& iu : rightOutput) {
            if (liveOut.contains(iu)) {
                AddInfoUnit(rightLive, iu);
            }
        }
    }

    for (const auto& [leftKey, rightKey] : JoinKeys) {
        AddInfoUnit(leftLive, leftKey);
        AddInfoUnit(rightLive, rightKey);
    }

    for (const auto& filter : JoinFilters) {
        TInfoUnitSet filterDeps;
        ctx.AddExpressionDeps(filter, filterDeps);
        for (const auto& iu : filterDeps) {
            if (leftOutput.contains(iu)) {
                AddInfoUnit(leftLive, iu);
            }
            if (rightOutput.contains(iu)) {
                AddInfoUnit(rightLive, iu);
            }
        }
    }

    ctx.AddLiveColumns(leftInput, leftLive);
    ctx.AddLiveColumns(rightInput, rightLive);
}

void TOpUnionAll::PropagateLiveness(ILivenessContext& ctx) {
    const TInfoUnitSet liveOut = ctx.GetLiveOut(this);
    ctx.AddLiveColumns(GetLeftInput(), liveOut);
    ctx.AddLiveColumns(GetRightInput(), liveOut);
}

void TOpLimit::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(LimitCond, inputLive);
    if (auto offsetCond = GetOffsetCond()) {
        ctx.AddExpressionDeps(*offsetCond, inputLive);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpSort::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive = ctx.GetLiveOut(this);
    for (const auto& sortElement : SortElements) {
        AddInfoUnit(inputLive, sortElement.SortColumn);
    }
    if (LimitCond) {
        ctx.AddExpressionDeps(*LimitCond, inputLive);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpAggregate::PropagateLiveness(ILivenessContext& ctx) {
    TInfoUnitSet inputLive;
    AddInfoUnits(inputLive, KeyColumns);
    for (const auto& traits : AggregationTraitsList) {
        AddInfoUnit(inputLive, traits.OriginalColName);
    }
    ctx.AddLiveColumns(GetInput(), inputLive);
}

void TOpCBOTree::PropagateLiveness(ILivenessContext& ctx) {
    for (const auto& child : Children) {
        ctx.AddLiveColumns(child, child->GetOutputIUs());
    }
}

void ComputePlanLiveness(TOpRoot& root) {
    TLogicalLiveness(root.PlanProps).Run(root);
}

} // namespace NKqp
} // namespace NKikimr
