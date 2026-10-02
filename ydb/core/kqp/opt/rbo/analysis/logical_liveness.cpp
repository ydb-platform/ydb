#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NKqp {

namespace {

// Local mode propagates over the whole plan, subplans included, in one pass.
// Global mode analyzes each subplan on its first call from a needed definition;
// the caller then demands the outer IDs the subplan captures.
class TLogicalLiveness: public ILivenessContext {
public:
    TLogicalLiveness(TPlanProps& props, ELivenessMode mode, bool pruneKeyColumns, EPruningScope scope)
        : ILivenessContext(mode, scope)
        , Props(props)
        , PruneKeyColumns(pruneKeyColumns)
    {
        Y_ENSURE(pruneKeyColumns || IsGlobal(), "Key-preserving demand is a global pruning policy");
    }

    void Run(TOpRoot& root) {
        const TOpTraversal traversal(IterateSubtreeWithSubplans(&root, Props).begin());
        for (const auto& iter : traversal) {
            iter.Current->Props.Analysis.LiveInByChild.reset();
            iter.Current->Props.Analysis.LiveOut.reset();
        }

        const auto& rootColumns = root.GetColumns().Unordered();
        if (!IsGlobal()) {
            Propagate(traversal, root, rootColumns);
            return;
        }

        // Entries no longer referenced by the plan may retain analysis from
        // a previous run. An engaged root LiveOut marks an evaluated call,
        // including EXISTS with no demanded columns.
        for (const auto& [id, subplan] : Props.Subplans) {
            subplan.Plan->Props.Analysis.LiveOut.reset();
        }
        Y_ENSURE(AnalyzeScope(root, rootColumns).Empty(), "Unbound live captures in root plan");
    }

    const TUnorderedIUs& GetLiveOut(const IOperator* op) const override {
        return NKikimr::NKqp::GetLiveOut(op);
    }

    void AddLiveInput(IOperator* op, ui32 childIndex, const TUnorderedIUs& columns) override {
        auto& child = *op->GetChild(childIndex);
        Y_ENSURE(columns.IsSubsetOf(child.GetOutputIUs()), "Liveness references an unavailable input ID");
        op->Props.Analysis.LiveInByChild->at(childIndex).UnionWith(columns);
        child.Props.Analysis.LiveOut->UnionWith(columns);
    }

    void AddExpressionDeps(const TExpression& expr, TUnorderedIUs& target) override {
        expr.BindPlanProps(&Props);
        const auto& raw = expr.GetRawInputIUs();
        const auto calls = Props.Subplans.CallsIn(raw);
        if (!IsGlobal()) {
            // Correlated deps are the callers' sources (DependentIUs), not the
            // subplans' captured locals.
            target.UnionWith(expr.GetInputIUs(false, true));
            for (const auto call : calls) {
                // EXISTS has no result, but its plan is still visited with empty
                // demand: rows, predicates, grouping and retained definitions
                // remain significant.
                const auto& subplan = Props.Subplans.At(call);
                if (subplan.ResultIU) {
                    subplan.Plan->Props.Analysis.LiveOut->Add(*subplan.ResultIU);
                }
            }
            return;
        }

        auto columns = raw;
        columns.Subtract(calls);
        target.UnionWith(columns);
        for (const auto call : calls) {
            const auto& subplan = Props.Subplans.At(call);
            target.UnionWith(subplan.Tuple.Unordered());
            target.UnionWith(AnalyzeSubplan(call, subplan));
        }
    }

    void AddCaptureDeps(const TUnorderedIUs& outer) override {
        if (IsGlobal()) {
            OuterDemand->UnionWith(outer);
        }
    }

private:
    // In a postorder of the plan DAG every operator follows its inputs and
    // subplans precede their callers. Reversed, all consumers of an operator,
    // including every Replicate port, run before it: one visit sees all demand.
    void Propagate(const TOpTraversal& traversal, IOperator& root, const TUnorderedIUs& output) {
        Initialize(traversal);
        // A subplan root may itself be a Map/Read with protected keys.
        root.Props.Analysis.LiveOut->UnionWith(output);
        SeedStageConnectionLiveness(traversal);
        for (auto it = traversal.rbegin(); it != traversal.rend(); ++it) {
            it->Current->PropagateLiveness(*this);
        }
    }

    void Initialize(const TOpTraversal& traversal) {
        for (const auto& iter : traversal) {
            auto& op = *iter.Current;
            // Capture-free producers can be shared by different subplans.
            // Their demands accumulate across scopes during this analysis run.
            if (IsGlobal() && op.Props.Analysis.LiveOut) {
                continue;
            }
            op.Props.Analysis.LiveInByChild.emplace(op.GetChildCount());
            auto& live = op.Props.Analysis.LiveOut.emplace();
            // Only Map/Read protect metadata keys.
            // Seed before propagation so retained definitions keep their inputs.
            if (!PruneKeyColumns && (op.Kind == EOperator::Map || op.Kind == EOperator::Source)) {
                Y_ENSURE(op.Props.Metadata, "Key-preserving liveness requires metadata");
                live = op.Props.Metadata->KeyColumns.Unordered();
            }
        }
    }

    // Global mode: returns the outer IDs a plan scope demands from its caller.
    TUnorderedIUs AnalyzeScope(IOperator& root, const TUnorderedIUs& output) {
        TUnorderedIUs outer;
        auto* previous = OuterDemand;
        OuterDemand = &outer;
        Y_DEFER { OuterDemand = previous; };

        // A call is analyzed before its caller's input, so live capture sources
        // flow outward immediately. Each scope still needs only one DAG pass.
        const TOpTraversal traversal(IterateSubtree(&root).begin());
        Propagate(traversal, root, output);

        for (const auto& iter : traversal) {
            if (iter.Current->Kind == EOperator::DependentJoin) {
                // Inlined correlations are supplied by a domain in this scope,
                // not by its caller.
                outer.Subtract(CastOperator<TOpDependentJoin>(*iter.Current).Dependencies);
            }
        }
        return outer;
    }

    const TUnorderedIUs& AnalyzeSubplan(TInfoUnitId id, const TSubplanEntry& subplan) {
        if (const auto* demand = SubplanDemand.Find(id)) {
            Y_ENSURE(*demand, "Cyclic subplan reference");
            return **demand;
        }
        SubplanDemand.Add(id, std::nullopt);
        TUnorderedIUs output;
        if (subplan.ResultIU) {
            output.Add(*subplan.ResultIU);
        }
        auto outer = AnalyzeScope(*subplan.Plan, output);
        // Nested calls can grow the cache; do not retain an iterator across them.
        auto& result = SubplanDemand.At(id);
        result.emplace(std::move(outer));
        return *result;
    }

    void SeedStageConnectionLiveness(const TOpTraversal& traversal) {
        for (const auto& iter : traversal) {
            const auto& parent = iter.Current;
            for (auto* child : parent->GetChildren()) {
                if (!parent->Props.StageId || !child->Props.StageId
                    || *parent->Props.StageId == *child->Props.StageId)
                {
                    continue;
                }

                const auto producerStageId = static_cast<ui32>(*child->Props.StageId);
                const auto consumerStageId = static_cast<ui32>(*parent->Props.StageId);
                const auto& connections = Props.StageGraph.GetConnections(producerStageId, consumerStageId);

                for (const auto& connection : connections) {
                    // A Replicate port produces only its own stage output.
                    if (child->Props.StageOutputIndex && connection->GetOutputIndex() != *child->Props.StageOutputIndex) {
                        continue;
                    }
                    // Stage connections seed the producer's LiveOut directly.
                    child->Props.Analysis.LiveOut->UnionWith(connection->GetUsedIUs());
                }
            }
        }
    }

    TPlanProps& Props;
    const bool PruneKeyColumns;
    TUnorderedIUs* OuterDemand = nullptr; // Borrowed from the active AnalyzeScope.
    // Outer demand of each analyzed subplan; std::nullopt while in progress.
    TMappedIUs<std::optional<TUnorderedIUs>> SubplanDemand;
};

// The live outputs of `op` that it passes through from the given child.
TUnorderedIUs LivePassthrough(IOperator* op, ui32 childIndex, const ILivenessContext& ctx) {
    auto live = ctx.GetLiveOut(op);
    live.IntersectWith(op->GetChild(childIndex)->GetOutputIUs());
    return live;
}

} // anonymous namespace

bool ILivenessContext::PrunesDefinitionsOf(const IOperator* op) const {
    return Scope == EPruningScope::AllDefinitions || op->Kind == EOperator::Map;
}

void IOperator::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

void IUnaryOperator::PropagateLiveness(ILivenessContext& ctx) {
    ctx.AddLiveInput(this, 0, LivePassthrough(this, 0, ctx));
}

void TOpRead::PropagateLiveness(ILivenessContext& ctx) {
    Y_UNUSED(ctx);
}

void TOpReplicate::PropagateLiveness(ILivenessContext& ctx) {
    ctx.AddLiveInput(this, 0, MapToInput(ctx.GetLiveOut(this)));
}

void TOpMap::PropagateLiveness(ILivenessContext& ctx) {
    auto inputLive = LivePassthrough(this, 0, ctx);
    for (const auto& [output, element] : MapElements.Items()) {
        if (ctx.NeedsDefinition(this, output)) {
            ctx.AddExpressionDeps(element.GetExpression(), inputLive);
        }
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpAddDependencies::PropagateLiveness(ILivenessContext& ctx) {
    // Captures are never pruned: every captured outer source stays live
    // while the subplan is evaluated.
    ctx.AddCaptureDeps(Dependencies.MappedIUs());
    IUnaryOperator::PropagateLiveness(ctx);
}

void TOpFilter::PropagateLiveness(ILivenessContext& ctx) {
    auto inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(FilterExpr, inputLive);
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpJoin::PropagateLiveness(ILivenessContext& ctx) {
    auto required = ctx.GetLiveOut(this);
    required.UnionWith(JoinKeys.Left());
    required.UnionWith(JoinKeys.Right());
    for (const auto& filter : JoinFilters) {
        ctx.AddExpressionDeps(filter, required);
    }

    auto leftLive = required;
    leftLive.IntersectWith(GetLeftInput()->GetOutputIUs());
    auto rightLive = std::move(required);
    rightLive.IntersectWith(GetRightInput()->GetOutputIUs());

    ctx.AddLiveInput(this, 0, leftLive);
    ctx.AddLiveInput(this, 1, rightLive);
}

void TOpDependentJoin::PropagateLiveness(ILivenessContext& ctx) {
    auto domainLive = LivePassthrough(this, 0, ctx);
    // Keep domain.
    domainLive.UnionWith(GetDomainColumns());
    // Captured locals belong to the body; domain IDs must not be injected into
    // its requirements merely because the corresponding value is equal.
    const auto inputLive = LivePassthrough(this, 1, ctx);

    ctx.AddLiveInput(this, 0, domainLive);
    ctx.AddLiveInput(this, 1, inputLive);
}

void TOpUnionAll::PropagateLiveness(ILivenessContext& ctx) {
    // Each retained row reads one input from every child.
    TVector<TUnorderedIUs> inputLive(GetChildCount());
    for (const auto& [output, row] : Columns.Items()) {
        if (ctx.NeedsDefinition(this, output)) {
            for (ui32 childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
                inputLive[childIndex].Add(row.Inputs[childIndex]);
            }
        }
    }
    for (ui32 childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        ctx.AddLiveInput(this, childIndex, inputLive[childIndex]);
    }
}

void TOpLimit::PropagateLiveness(ILivenessContext& ctx) {
    auto inputLive = ctx.GetLiveOut(this);
    ctx.AddExpressionDeps(LimitCond, inputLive);
    if (OffsetCond) {
        ctx.AddExpressionDeps(*OffsetCond, inputLive);
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpSort::PropagateLiveness(ILivenessContext& ctx) {
    auto inputLive = ctx.GetLiveOut(this);
    inputLive.UnionWith(SortElements.Unordered());
    if (LimitCond) {
        ctx.AddExpressionDeps(*LimitCond, inputLive);
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpTableLookup::PropagateLiveness(ILivenessContext& ctx) {
    TUnorderedIUs inputLive = LookupKeys.Unordered();
    if (Prefix) {
        // The prefix equalities are checked on the left row before the lookup.
        inputLive.UnionWith(Prefix->Equalities.Unordered());
    }
    if (IsJoin()) {
        inputLive.UnionWith(LivePassthrough(this, 0, ctx));
        inputLive.UnionWith(ResidualJoinKeys.Left());
    }
    if (FetchedRowFilter) {
        // The filter also reads fetched columns, which are not inputs.
        TUnorderedIUs filterDeps;
        ctx.AddExpressionDeps(*FetchedRowFilter, filterDeps);
        filterDeps.IntersectWith(GetInput()->GetOutputIUs());
        inputLive.UnionWith(filterDeps);
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpIndexLookupJoin::PropagateLiveness(ILivenessContext& ctx) {
    auto inputLive = ctx.GetLiveOut(this);
    inputLive.UnionWith(JoinKeys.Left());
    inputLive.UnionWith(JoinKeys.Right());
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpAggregate::PropagateLiveness(ILivenessContext& ctx) {
    TUnorderedIUs inputLive = KeyColumns.Unordered();
    for (const auto& [output, traits] : Aggregations.Items()) {
        // A DistinctAll never loses aggregations; see PruneOutputs.
        if (IsDistinctAll() || ctx.NeedsDefinition(this, output)) {
            inputLive.Add(traits.Input);
        }
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpGroupingSets::PropagateLiveness(ILivenessContext& ctx) {
    TUnorderedIUs inputLive;
    for (const auto& [output, input] : Columns.Items()) {
        if (ctx.NeedsDefinition(this, output)) {
            inputLive.Add(input);
        }
    }
    for (const auto& [output, input] : GroupingIndicators.Items()) {
        if (ctx.NeedsDefinition(this, output)) {
            inputLive.Add(input);
        }
    }
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpWindow::PropagateLiveness(ILivenessContext& ctx) {
    // Window functions are never pruned, so every function keeps its inputs
    // live in both modes.
    auto inputLive = LivePassthrough(this, 0, ctx);
    inputLive.UnionWith(PartitionKeys.Unordered());
    inputLive.UnionWith(SortElements.Unordered());
    inputLive.UnionWith(WindowFuncs.MappedIUs());
    ctx.AddLiveInput(this, 0, inputLive);
}

void TOpCBOTree::PropagateLiveness(ILivenessContext& ctx) {
    // CBO's packed tree is opaque to this analysis: all boundary columns stay live.
    for (ui32 childIndex = 0; childIndex < GetChildCount(); ++childIndex) {
        ctx.AddLiveInput(this, childIndex, GetChild(childIndex)->GetOutputIUs());
    }
}

void TOpTableEffect::PropagateLiveness(ILivenessContext& ctx) {
    ctx.AddLiveInput(this, 0, GetColumns().Unordered());
}

void ComputePlanLiveness(TOpRoot& root, ELivenessMode mode, bool pruneKeyColumns, EPruningScope scope) {
    TLogicalLiveness(root.PlanProps, mode, pruneKeyColumns, scope).Run(root);
}

const TUnorderedIUs& GetLiveIn(const IOperator* op, ui32 childIndex) {
    Y_ENSURE(op);
    Y_ENSURE(
        op->Props.Analysis.LiveInByChild.has_value(),
        "Liveness requested for an operator without computed input liveness, kind: " << static_cast<ui32>(op->Kind));
    const auto& liveInByChild = *op->Props.Analysis.LiveInByChild;
    Y_ENSURE(childIndex < liveInByChild.size());
    return liveInByChild[childIndex];
}

const TUnorderedIUs& GetLiveOut(const IOperator* op) {
    Y_ENSURE(op);
    Y_ENSURE(
        op->Props.Analysis.LiveOut.has_value(),
        "Liveness requested for an operator without computed liveness, kind: " << static_cast<ui32>(op->Kind));
    return *op->Props.Analysis.LiveOut;
}

} // namespace NKqp
} // namespace NKikimr
