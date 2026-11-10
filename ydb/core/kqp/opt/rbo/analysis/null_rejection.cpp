#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#include <functional>
#include <memory>

namespace NKikimr::NKqp {
namespace {

// A consumer's condition on its input: true proves rows with these NULL
// columns cannot affect the result. Compose this condition downward through
// each operator, then test an outer join's unmatched rows against it.
// Sharing contexts avoids copying the ancestor chain at every branch.
using TRejectsNulls = std::function<bool(const TUnorderedIUs&)>;
using TContext = std::shared_ptr<const TRejectsNulls>;

TContext NoRejection() {
    static const auto context = std::make_shared<const TRejectsNulls>([](const TUnorderedIUs&) { return false; });
    return context;
}

TContext AllConsumers(TVector<TContext> consumers) {
    Y_ENSURE(!consumers.empty());
    if (consumers.size() == 1) {
        return std::move(consumers.front());
    }
    return std::make_shared<const TRejectsNulls>([consumers = std::move(consumers)](const TUnorderedIUs& nulls) {
        return AllOf(consumers, [&](const auto& consumer) { return (*consumer)(nulls); });
    });
}

bool JoinRejectsNulls(const TOpJoin& join, const TUnorderedIUs& nulls) {
    for (const auto& key : join.JoinKeys.Items()) {
        if (!key.EqualNulls && (nulls.Contains(key.first) || nulls.Contains(key.second))) {
            return true;
        }
    }
    return AnyOf(join.JoinFilters, [&](const auto& filter) { return filter.RejectsNulls(nulls); });
}

} // namespace

// Temporary state for one operator. Its virtual method derives conditions for
// its inputs. Unhandled inputs keep the conservative, unconstrained context.
class TNullRejectionContext {
public:
    TNullRejectionContext(TContext consumer, size_t inputCount)
        : Consumer(std::move(consumer))
        , Inputs(inputCount, NoRejection())
    {}

    const TContext Consumer;
    TVector<TContext> Inputs;
};

void IOperator::PropagateNullRejection(TNullRejectionContext& ctx) {
    Y_UNUSED(ctx);
    // LIMIT, aggregation, windows, Replicate and opaque CBO trees can observe
    // rows before their consumers reject them. Start fresh below the boundary;
    // predicates encountered further down can still establish their own proof.
}

void TOpFilter::PropagateNullRejection(TNullRejectionContext& ctx) {
    ctx.Inputs[0] = std::make_shared<const TRejectsNulls>([this, consumer = ctx.Consumer](const TUnorderedIUs& nulls) {
        return GetFilterExpression().RejectsNulls(nulls) || (*consumer)(nulls);
    });
}

void TOpMap::PropagateNullRejection(TNullRejectionContext& ctx) {
    ctx.Inputs[0] = std::make_shared<const TRejectsNulls>([this, consumer = ctx.Consumer](const TUnorderedIUs& nulls) {
        auto mappedNulls = nulls;
        for (const auto& [id, element] : GetMapElements().Items()) {
            if (element.GetExpression().IsNullWhen(nulls)) {
                mappedNulls.Add(id);
            }
        }
        return (*consumer)(mappedNulls);
    });
}

void TOpJoin::PropagateNullRejection(TNullRejectionContext& ctx) {
    const bool outer = JoinKind == "Left" || JoinKind == "Right" || JoinKind == "Full";
    if (outer) {
        const auto rejects = [&](size_t nullInput) {
            const auto& nulls = GetChild(nullInput)->GetOutputIUs();
            return !nulls.Empty() && (*ctx.Consumer)(nulls);
        };
        Props.Analysis.OuterJoinNullRejection = TOuterJoinNullRejection{
            .RejectsUnmatchedLeft = (JoinKind == "Left" || JoinKind == "Full") && rejects(1),
            .RejectsUnmatchedRight = (JoinKind == "Right" || JoinKind == "Full") && rejects(0),
        };
    }

    for (size_t child = 0; child < GetChildCount(); ++child) {
        // ON rejects rows only on non-preserved inputs.
        const bool rejectsInput = JoinKind == "Inner" || JoinKind == "Cross" || JoinKind == "LeftSemi" || JoinKind == "RightSemi" ||
            (child == 1 && (JoinKind == "Left" || JoinKind == "LeftOnly")) ||
            (child == 0 && (JoinKind == "Right" || JoinKind == "RightOnly"));
        ctx.Inputs[child] = std::make_shared<const TRejectsNulls>(
            [this, consumer = ctx.Consumer, child, rejectsInput, outer](const TUnorderedIUs& nulls) {
                auto outputNulls = nulls;
                if (JoinRejectsNulls(*this, nulls)) {
                    if (rejectsInput) {
                        return true;
                    }
                    // A preserved row that cannot match also null-extends the
                    // other input. The consumer may reject that combined case.
                    if (outer) {
                        outputNulls.UnionWith(GetChild(child ^ 1)->GetOutputIUs());
                    }
                }
                outputNulls.IntersectWith(GetOutputIUs());
                return !outputNulls.Empty() && (*consumer)(outputNulls);
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
    THashMap<IOperator*, TVector<TContext>> incoming;
    incoming[root.GetInput().Get()].push_back(NoRejection());

    // Reverse post-order visits every consumer before its inputs, including
    // shared producers. Ordinary pre-order would visit a shared input too early.
    for (auto it = traversal.rbegin(); it != traversal.rend(); ++it) {
        auto& op = *it->Current;
        TNullRejectionContext ctx(AllConsumers(std::move(incoming.at(&op))), op.GetChildCount());
        op.Props.Analysis.OuterJoinNullRejection.reset();
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
