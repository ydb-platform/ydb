#include "kqp_operator.h"
#include "kqp_rbo_utils.h"

namespace NKikimr::NKqp {

namespace {

void AddSubstitution(TSubstitutions& target, TInfoUnitId from, TInfoUnitId to) {
    if (const auto* existing = target.Find(from)) {
        Y_ENSURE(*existing == to, "Conflicting consumer substitutions for " << from);
    } else {
        target.Add(from, to);
    }
}

// Inspect stored contracts, not GetOutputIUs(): the rule may already have taken
// children from its old root, and no schema can be recomputed until it returns.
bool ForwardsInput(IOperator& op, ui32 child, TInfoUnitId id) {
    switch (op.Kind) {
        case EOperator::Map:
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::Window:
        case EOperator::AddDependencies:
        case EOperator::DependentJoin:
            return true;
        case EOperator::Replicate:
            return CastOperator<TOpReplicate>(op).IsPrimary();
        case EOperator::Aggregate: {
            const auto& aggregate = CastOperator<TOpAggregate>(op);
            return !aggregate.IsDistinctAll() && aggregate.GetKeyColumns().Unordered().Contains(id);
        }
        case EOperator::Join: {
            const auto& kind = CastOperator<TOpJoin>(op).JoinKind;
            return child == 0 ? JoinOutputsLeft(kind) : JoinOutputsRight(kind);
        }
        case EOperator::TableLookup:
            return CastOperator<TOpTableLookup>(op).IsJoin();
        case EOperator::IndexLookupJoin: {
            const auto& join = CastOperator<TOpIndexLookupJoin>(op);
            return JoinOutputsRight(join.JoinKind) || !join.GetTableLookup().GetColumns().Contains(id);
        }
        case EOperator::UnionAll:
        case EOperator::GroupingSets:
        case EOperator::TableEffect:
        case EOperator::Root:
            return false;
        default:
            Y_ENSURE(false, "Unsupported consumer for IU rebinding: " << op.GetExplainName());
    }
}

void Invalidate(IOperator& op) {
    op.Props.OutputIUs.reset();
    op.Props.Metadata.reset();
    op.Props.Statistics.reset();
    op.Props.Cost.reset();
    op.Props.ClearLogicalAnalysis();
    op.Type = nullptr;
    // Parents are the traversal's edges. The rule engine rebuilds them after
    // installing the replacement; clearing them here would cut propagation short.
}

struct TConsumer {
    size_t PendingInputs = 0;
    TSubstitutions Inputs;
    TSubstitutions Outputs;
};

} // anonymous namespace

void RebindConsumers(IOperator& oldRoot, const TSubstitutions& substitutions, TSubplans& subplans) {
    TSubstitutions replacements;
    for (const auto& [from, to] : substitutions.Items()) {
        Y_ENSURE(to != TUnorderedIUs::InvalidBit, "Invalid replacement ID");
        Y_ENSURE(!subplans.Contains(from) && !subplans.Contains(to), "Subplan call bindings cannot be replaced");
        if (from != to) {
            replacements.Add(from, to);
        }
    }
    if (replacements.Keys().Empty()) {
        return;
    }

    THashMap<IOperator*, TInfoUnitId> subplanRoots;
    for (const auto& [binding, entry] : subplans) {
        Y_ENSURE(entry.Plan, "Rebind consumers before detaching a subplan root");
        subplanRoots.emplace(entry.Plan.get(), binding);
    }

    // Snapshot the upward closure, including ancestors beyond defining
    // boundaries: their IDs stay fixed but their cached properties may not.
    THashMap<IOperator*, TConsumer> consumers;
    TVector<IOperator*> pending{&oldRoot};
    consumers[&oldRoot].Outputs = std::move(replacements);
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& op = *pending[i];
        Y_ENSURE(op.Kind != EOperator::CBOTree, "Consumer rebinding requires unpacked CBO trees");
        Y_ENSURE(!op.Props.StageId && !op.Props.LeftShuffleBy && !op.Props.RightShuffleBy && !op.Props.OrderEnforcer,
            "Consumer rebinding must precede physical assignment");
        Y_ENSURE(!op.Parents.empty() || op.Kind == EOperator::Root || subplanRoots.contains(&op),
            "Consumer rebinding requires current parents and registered scope roots");
        for (const auto& [parent, child] : op.Parents) {
            const auto [it, inserted] = consumers.try_emplace(parent);
            ++it->second.PendingInputs;
            if (inserted) {
                pending.push_back(parent);
            }
        }
    }

    // One call entry can be mentioned repeatedly. Gather its substitutions and
    // apply them once, preserving simultaneous replacement rather than chaining.
    TMappedIUs<TSubstitutions> callInputs;
    pending.assign(1, &oldRoot);
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& op = *pending[i];
        auto& consumer = consumers.at(&op);
        if (&op != &oldRoot) {
            if (!consumer.Inputs.Keys().Empty()) {
                for (const auto binding : op.GetSubplanIUs(subplans)) {
                    auto* inputs = callInputs.Find(binding);
                    if (!inputs) {
                        inputs = &callInputs.Add(binding);
                    }
                    for (const auto& [from, to] : consumer.Inputs.Items()) {
                        AddSubstitution(*inputs, from, to);
                    }
                }
                if (op.Kind == EOperator::Replicate && !CastOperator<TOpReplicate>(op).IsPrimary()) {
                    consumer.Outputs = CastOperator<TOpReplicate>(op).RebindInputs(consumer.Inputs);
                } else {
                    op.RenameUsedIUs(consumer.Inputs);
                }
            }
            Invalidate(op);
        }

        if (const auto scope = subplanRoots.find(&op); scope != subplanRoots.end()) {
            subplans.RebindResult(scope->second, consumer.Outputs);
        }
        for (const auto& [parent, child] : op.Parents) {
            auto& next = consumers.at(parent);
            for (const auto& [from, to] : consumer.Outputs.Items()) {
                AddSubstitution(next.Inputs, from, to);
                if (ForwardsInput(*parent, child, from)) {
                    AddSubstitution(next.Outputs, from, to);
                }
            }
            // A shared producer can reconverge above a Replicate. Rebind each
            // consumer once, after every affected input has contributed.
            if (--next.PendingInputs == 0) {
                pending.push_back(parent);
            }
        }
    }
    Y_ENSURE(pending.size() == consumers.size(), "Cycle in consumer parent links");
    for (const auto& [binding, inputs] : callInputs.Items()) {
        subplans.RebindInputs(binding, inputs);
    }
}

} // namespace NKikimr::NKqp
