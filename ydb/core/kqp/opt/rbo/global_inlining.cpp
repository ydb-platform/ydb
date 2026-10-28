#include "kqp_rbo.h"

namespace NKikimr::NKqp {

namespace {

void EliminateCopies(TOpRoot& root, TExprContext& ctx) {
    const TOpTraversal traversal(IterateSubtreeWithSubplans(&root, root.PlanProps).begin());
    TSubstitutions directCopies;
    // Read the original schemas before changing any port. A subplan result
    // reference is an evaluation, not a copy of an ordinary input field.
    for (const auto& item : traversal) {
        auto& op = *item.Current;
        Y_ENSURE(op.Kind != EOperator::CBOTree, "Global inlining requires unpacked CBO trees");
        Y_ENSURE(op.Kind != EOperator::GroupingSets, "Global inlining requires expanded grouping sets");
        Y_ENSURE(!op.Props.StageId, "Global inlining must precede stage assignment");
        if (op.Kind != EOperator::Map) {
            continue;
        }
        auto& map = CastOperator<TOpMap>(op);
        const auto& inputs = map.GetInput()->GetOutputIUs();
        for (const auto& [id, element] : map.GetMapElements().Items()) {
            if (element.IsColumnAccess() && inputs.Contains(element.GetColumnAccess())) {
                directCopies.Add(id, element.GetColumnAccess());
            }
        }
    }
    if (directCopies.Keys().Empty()) {
        return;
    }

    TSubstitutions copies;
    // Dependency-first order flattens each chain once. Replicate ports translate the
    // equivalence into their own namespaces; captures never enter this table.
    // Do not refresh output sets while the schemas are being rewritten.
    for (const auto& item : traversal) {
        auto& op = *item.Current;
        if (op.Kind == EOperator::Map) {
            for (const auto id : CastOperator<TOpMap>(op).GetMapElements().Keys()) {
                const auto* direct = directCopies.Find(id);
                if (!direct || *direct == id) {
                    continue;
                }
                const auto origin = Substitute(*direct, copies);
                Y_ENSURE(origin != id && !copies.Keys().Contains(origin), "Invalid copy dependency order");
                copies.Add(id, origin);
            }
        } else if (op.Kind == EOperator::Replicate) {
            const auto portCopies = CastOperator<TOpReplicate>(op).RebindInputs(copies);
            for (const auto& [copy, origin] : portCopies.Items()) {
                copies.Add(copy, origin);
            }
        }
    }

    // Rewrite all uses before deleting any definition. Map RHSs stay
    // simultaneous; this never composes one RHS into a sibling RHS.
    for (const auto& item : traversal) {
        auto& op = *item.Current;
        if (!copies.Keys().Empty()) {
            op.RenameUsedIUs(copies);
        }
        if (op.Kind == EOperator::Map) {
            auto& map = CastOperator<TOpMap>(op);
            auto keep = map.GetMapElements().Keys();
            keep.Subtract(directCopies.Keys());
            map.PruneOutputs(keep, ctx);
        }
    }
    root.PlanProps.Subplans.RenameExternalReferences(copies);
    root.PlanProps.Subplans.RebindResults(copies);
    for (const auto& [id, entry] : root.PlanProps.Subplans) {
        root.PlanProps.Subplans.RefreshDependencies(id);
    }
}

} // anonymous namespace

TGlobalInliningStage::TGlobalInliningStage(TString stageName)
    : IRBOStage(std::move(stageName))
{
    Props = ERuleProperties::RequireOutputIUs;
}

void TGlobalInliningStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    // Collapse singleton ports before collecting copies, including their new
    // binding Maps in this same pass.
    FinishLogicalRewrite(root, ctx.ExprCtx);
    EliminateCopies(root, ctx.ExprCtx);
    FinishLogicalRewrite(root, ctx.ExprCtx);
}

} // namespace NKikimr::NKqp
