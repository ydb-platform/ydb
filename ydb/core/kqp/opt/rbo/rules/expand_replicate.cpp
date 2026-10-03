#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <algorithm>

namespace NKikimr::NKqp {

namespace {

// Nested producers multiply in size when copied, so bound every copy.
constexpr size_t MaxCopiedOperators = 1000;

// Matches AllowWithSpilling, which the query compiler sets on every stage:
// only then can a stage with several outputs spill its channels.
bool AllowsChannelSpilling(const NOpt::TKqpOptimizeContext& kqpCtx) {
    const auto& config = *kqpCtx.Config;
    return config.GetEnableQueryServiceSpilling() && (kqpCtx.IsGenericQuery() || kqpCtx.IsScanQuery())
        && config.SpillingEnabled();
}

bool CallsNonDeterministicFunction(const TExprNode::TPtr& node) {
    static const auto functions = NOpt::GetNonDeterministicFunctions();
    return node && FindNode(node, [](const TExprNode::TPtr& child) {
        return child->IsCallable() && functions.contains(child->Content());
    });
}

// Whether a copy of `op` may return other rows than the original does.
bool MayDiffer(IOperator& op) {
    for (const auto& expression : op.GetExpressions()) {
        if (CallsNonDeterministicFunction(expression.get().Node)) {
            return true;
        }
    }
    switch (op.Kind) {
        // These depend on the order of rows, which may differ between runs.
        case EOperator::Limit:
        case EOperator::Window:
            return true;
        case EOperator::Sort:
            return CastOperator<TOpSort>(op).IsTopSort();
        case EOperator::EmptySource:
            return CallsNonDeterministicFunction(CastOperator<TOpEmptySource>(op).Input);
        case EOperator::Aggregate:
            // SOME returns any value of its group.
            return std::ranges::any_of(CastOperator<TOpAggregate>(op).GetAggregationTraits().Items(),
                [](const auto& item) { return item.second.AggFunction == "some"; });
        default:
            return false;
    }
}

// Whether every consumer may evaluate its own copy of `producer`: copies return
// the same rows, stay small and call no subplan, which cannot have two callers.
bool MayCopy(IOperator& producer, const TSubplans& subplans) {
    size_t count = 0;
    for (const auto& item : IterateSubtree(&producer)) {
        if (++count > MaxCopiedOperators || MayDiffer(*item.Current) || !item.Current->GetSubplanIUs(subplans).Empty()) {
            return false;
        }
    }
    return true;
}

// A copy of the producer that defines the IDs of a non-primary port, or
// nullptr if the producer cannot be copied.
TIntrusivePtr<IOperator> CopyProducer(TOpReplicate& port, TExprContext& ctx, TPlanProps& props) {
    auto& producer = *port.GetInput();
    const auto& bindings = port.GetRebindings();
    TSubstitutions renames;
    for (const auto source : producer.GetOutputIUs()) {
        renames.Add(source, bindings.At(source));
    }
    auto copy = producer.Copy(props.InfoUnitRegistry, renames);
    if (!copy) {
        return nullptr;
    }
    // A copied read keeps its column names, so it may turn down a port ID.
    TMapIUs copies;
    for (const auto source : producer.GetOutputIUs()) {
        const auto output = bindings.At(source);
        if (const auto copied = renames.At(source); copied != output) {
            copies.Add(output, MakeColumnAccess(copied, port.Pos, &ctx, &props));
        }
    }
    if (copies.Keys().Empty()) {
        return copy;
    }
    return MakeIntrusive<TOpMap>(std::move(copy), port.Pos, std::move(copies));
}

} // anonymous namespace

bool TExpandReplicateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Replicate;
}

bool TExpandReplicateRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Replicate || AllowsChannelSpilling(ctx.KqpCtx)) {
        return false;
    }
    // The last reachable port reads the producer itself.
    if (TOpReplicate::TryCollapse(input, ctx.ExprCtx, props)) {
        return true;
    }
    // The primary port keeps the producer until the others have their copies.
    auto& port = CastOperator<TOpReplicate>(*input);
    if (port.IsPrimary() || !MayCopy(*port.GetInput(), props.Subplans)) {
        return false;
    }
    auto copy = CopyProducer(port, ctx.ExprCtx, props);
    if (!copy) {
        return false;
    }
    input = std::move(copy);
    return true;
}

} // namespace NKikimr::NKqp
