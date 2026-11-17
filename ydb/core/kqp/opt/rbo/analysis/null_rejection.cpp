#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NKikimr::NKqp {
namespace {

TNullRejection NoRejection() {
    static const TNullRejection context([](const TUnorderedIUs&) { return false; });
    return context;
}

TNullRejection AllConsumers(TVector<TNullRejection> consumers) {
    Y_ENSURE(!consumers.empty());
    if (consumers.size() == 1) {
        return std::move(consumers.front());
    }
    return TNullRejection([consumers = std::move(consumers)](const TUnorderedIUs& nulls) {
        return AllOf(consumers, [&](const auto& consumer) { return consumer.RejectsNulls(nulls); });
    });
}

TNullRejection JoinCondition(const TOpJoin& join) {
    return TNullRejection([keys = join.JoinKeys.Items(), filters = join.JoinFilters](const TUnorderedIUs& nulls) {
        for (const auto& key : keys) {
            if (!key.EqualNulls && (nulls.Contains(key.first) || nulls.Contains(key.second))) {
                return true;
            }
        }
        return AnyOf(filters, [&](const auto& filter) { return filter.RejectsNulls(nulls); });
    });
}

} // namespace

// Temporary state for one operator. Its virtual method derives conditions for
// its inputs. Unhandled inputs keep the conservative, unconstrained context.
class TNullRejectionContext {
public:
    TNullRejectionContext(TNullRejection consumer, size_t inputCount)
        : Consumer(std::move(consumer))
        , Inputs(inputCount, NoRejection())
    {}

    const TNullRejection Consumer;
    TVector<TNullRejection> Inputs;
};

void IOperator::PropagateNullRejection(TNullRejectionContext& ctx) {
    Y_UNUSED(ctx);
    // LIMIT, aggregation, windows, Replicate and opaque CBO trees can observe
    // rows before their consumers reject them. Start fresh below the boundary;
    // predicates encountered further down can still establish their own proof.
}

void TOpFilter::PropagateNullRejection(TNullRejectionContext& ctx) {
    ctx.Inputs[0] = TNullRejection([filter = GetFilterExpression(), consumer = ctx.Consumer](const TUnorderedIUs& nulls) {
        return filter.RejectsNulls(nulls) || consumer.RejectsNulls(nulls);
    });
}

void TOpMap::PropagateNullRejection(TNullRejectionContext& ctx) {
    ctx.Inputs[0] = TNullRejection([elements = GetMapElements().Items(), consumer = ctx.Consumer](const TUnorderedIUs& nulls) {
        auto mappedNulls = nulls;
        for (const auto& [id, element] : elements) {
            if (element.GetExpression().IsNullWhen(nulls)) {
                mappedNulls.Add(id);
            }
        }
        return consumer.RejectsNulls(mappedNulls);
    });
}

void TOpJoin::PropagateNullRejection(TNullRejectionContext& ctx) {
    const bool outer = JoinKind == "Left" || JoinKind == "Right" || JoinKind == "Full";
    const auto condition = JoinCondition(*this);

    for (size_t child = 0; child < GetChildCount(); ++child) {
        // ON rejects rows only on non-preserved inputs.
        const bool rejectsInput = JoinKind == "Inner" || JoinKind == "Cross" || JoinKind == "LeftSemi" || JoinKind == "RightSemi" ||
            (child == 1 && (JoinKind == "Left" || JoinKind == "LeftOnly")) ||
            (child == 0 && (JoinKind == "Right" || JoinKind == "RightOnly"));
        ctx.Inputs[child] = TNullRejection(
            [condition, consumer = ctx.Consumer, rejectsInput, outer,
             otherOutput = GetChild(child ^ 1)->GetOutputIUs(), output = GetOutputIUs()](const TUnorderedIUs& nulls) {
                auto outputNulls = nulls;
                if (condition.RejectsNulls(nulls)) {
                    if (rejectsInput) {
                        return true;
                    }
                    // A preserved row that cannot match also null-extends the
                    // other input. The consumer may reject that combined case.
                    if (outer) {
                        outputNulls.UnionWith(otherOutput);
                    }
                }
                outputNulls.IntersectWith(output);
                return !outputNulls.Empty() && consumer.RejectsNulls(outputNulls);
            });
    }
}

void TOpSort::PropagateNullRejection(TNullRejectionContext& ctx) {
    if (!IsTopSort()) {
        ctx.Inputs[0] = ctx.Consumer;
    }
}

void ComputePlanNullRejection(TOpRoot& root) {
    auto traversal = root.SnapshotTraversal();
    THashMap<IOperator*, TVector<TNullRejection>> incoming;
    root.Props.Analysis.NullRejection = NoRejection();
    incoming[root.GetInput().Get()].push_back(*root.Props.Analysis.NullRejection);

    // Reverse post-order visits every consumer before its inputs, including
    // shared producers. Ordinary pre-order would visit a shared input too early.
    for (auto it = traversal.rbegin(); it != traversal.rend(); ++it) {
        auto& op = *it->Current;
        TNullRejectionContext ctx(AllConsumers(std::move(incoming.at(&op))), op.GetChildCount());
        op.Props.Analysis.NullRejection = ctx.Consumer;
        op.PropagateNullRejection(ctx);
        for (size_t child = 0; child < op.GetChildCount(); ++child) {
            incoming[op.GetChild(child).Get()].push_back(std::move(ctx.Inputs[child]));
        }
        // A caller's row predicates do not constrain the subplan's rows.
        // Include every active reference, even if its producer is also shared
        // through ordinary child edges or through another subplan binding.
        for (const auto binding : op.GetSubplanIUs(root.PlanProps.Subplans)) {
            incoming[root.PlanProps.Subplans.At(binding).Plan.Get()].push_back(NoRejection());
        }
    }
}

} // namespace NKikimr::NKqp
