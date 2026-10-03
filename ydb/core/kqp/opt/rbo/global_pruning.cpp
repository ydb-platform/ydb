#include "kqp_rbo.h"

namespace NKikimr::NKqp {

bool IOperator::PruneOutputs(const TUnorderedIUs&, TExprContext&) {
    return false;
}

bool TOpRead::PruneOutputs(const TUnorderedIUs& liveOut, TExprContext& ctx) {
    auto keep = GetRequiredColumns(ctx);
    keep.UnionWith(liveOut);
    return Columns_.IntersectWith(keep);
}

bool TOpMap::PruneOutputs(const TUnorderedIUs& liveOut, TExprContext&) {
    return MapElements.RetainKeys(liveOut);
}

bool TOpAggregate::PruneOutputs(const TUnorderedIUs& liveOut, TExprContext&) {
    // Metadata treats a DistinctAll's complete output tuple as a unique key.
    // A subset need not remain unique.
    if (IsDistinctAll()) {
        return false;
    }
    // All grouping/distinct keys remain: they determine multiplicity even when
    // no aggregate result or key is consumed. A keyless aggregate may lose all
    // results; the rewrite boundary then replaces it with a one-row source.
    return Aggregations.RetainKeys(liveOut);
}

bool TOpGroupingSets::PruneOutputs(const TUnorderedIUs& liveOut, TExprContext&) {
    const bool columnsChanged = Columns.RetainKeys(liveOut);
    const bool indicatorsChanged = GroupingIndicators.RetainKeys(liveOut);
    if (columnsChanged || indicatorsChanged) {
        Props.OutputIUs.reset();
    }
    return columnsChanged || indicatorsChanged;
}

bool TOpUnionAll::PruneOutputs(const TUnorderedIUs& liveOut, TExprContext&) {
    return Columns.RetainKeys(liveOut);
}

TGlobalPruningStage::TGlobalPruningStage(TString stageName, bool pruneKeyColumns, EPruningScope scope)
    : IRBOStage(std::move(stageName))
    , PruneKeyColumns(pruneKeyColumns)
    , Scope(scope)
{
    Props = PruneKeyColumns ? ERuleProperties::RequireOutputIUs : ERuleProperties::RequireMetadata;
}

void TGlobalPruningStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    auto& subplans = root.PlanProps.Subplans;
    {
        const auto traversal = root.SnapshotTraversal();
        for (const auto& item : traversal) {
            Y_ENSURE(item.Current->Kind != EOperator::CBOTree,
                "Global pruning requires unpacked CBO trees");
            Y_ENSURE(!item.Current->Props.StageId, "Global pruning must precede stage assignment");
        }
        ComputePlanLiveness(root, ELivenessMode::Global, PruneKeyColumns, Scope);
        // Remove definitions everywhere before deleting nodes or subplan entries.
        // Disengaged LiveOut means an inactive subplan, not an empty row demand.
        for (const auto& item : traversal) {
            auto& op = *item.Current;
            if (op.Props.Analysis.LiveOut) {
                if (Scope == EPruningScope::AllDefinitions || op.Kind == EOperator::Map) {
                    op.PruneOutputs(GetLiveOut(&op), ctx.ExprCtx);
                }
                if (item.SubplanIU && !item.Parent) {
                    // Postorder: captures are now pruned, and callers have not
                    // been visited yet. Lookup predicates can use these IDs too.
                    subplans.RefreshDependencies(*item.SubplanIU);
                }
            }
        }
    }

    TUnorderedIUs unusedSubplans;
    for (const auto& [id, subplan] : subplans) {
        if (!subplan.Plan->Props.Analysis.LiveOut) {
            unusedSubplans.Add(id);
        }
    }
    for (const auto id : unusedSubplans) {
        subplans.Remove(id);
    }
    // Never let ordinary rules consume the weaker global liveness result.
    FinishLogicalRewrite(root, ctx.ExprCtx);

    // A result-less scalar aggregate drops its input, and with it the calls to
    // any subplan only that input used.
    TUnorderedIUs reachable;
    for (const auto& item : IterateSubtreeWithSubplans(&root, root.PlanProps)) {
        if (item.SubplanIU) {
            reachable.Add(*item.SubplanIU);
        }
    }
    unusedSubplans.Clear();
    for (const auto& [id, subplan] : subplans) {
        if (!reachable.Contains(id)) {
            unusedSubplans.Add(id);
        }
    }
    for (const auto id : unusedSubplans) {
        subplans.Remove(id);
    }
}

} // namespace NKikimr::NKqp
