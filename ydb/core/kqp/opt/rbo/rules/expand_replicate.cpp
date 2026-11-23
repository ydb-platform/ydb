#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/rbo/copy_logical_subtree.h>

namespace NKikimr::NKqp {

namespace {

// A copy of the producer that defines the IDs of a non-primary port.
TIntrusivePtr<IOperator> CopyProducer(TOpReplicate& port, TExprContext& ctx, TPlanProps& props) {
    auto& producer = *port.GetInput();
    const auto& bindings = port.GetRebindings();
    TSubstitutions renames;
    for (const auto source : producer.GetOutputIUs()) {
        renames.Add(source, bindings.At(source));
    }
    auto copy = producer.Copy(props, renames);
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
    if (input->Kind != EOperator::Replicate) {
        return false;
    }
    // The last reachable port reads the producer itself.
    if (TOpReplicate::TryCollapse(input, ctx.ExprCtx, props)) {
        return true;
    }
    // The primary port keeps the producer until the others have their copies.
    auto& port = CastOperator<TOpReplicate>(*input);
    if (port.IsPrimary() || !CanDuplicateSubtree(*port.GetInput(), props.Subplans)) {
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
