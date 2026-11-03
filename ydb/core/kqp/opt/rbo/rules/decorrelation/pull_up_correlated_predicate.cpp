#include "dependent_join_pushdown.h"

#include "../kqp_rules_include.h"
#include <ydb/core/kqp/opt/rbo/copy_logical_subtree.h>

namespace NKikimr {
namespace NKqp {

namespace {

struct TSubplanCall {
    TInfoUnitId Binding;
    const TSubplanEntry& Entry;
    bool Negated;
};

// A subplan split for its join with the caller: the right side of the join and the join conditions.
struct TPulledUpSubplan {
    TIntrusivePtr<IOperator> Input;
    TJoinIUs Keys;
    TVector<TExpression> Filters;
};

// Matches [NOT] call, where call is a correlated EXISTS or IN subplan.
std::optional<TSubplanCall> MatchCorrelatedCall(const TExpression& conjunct, const TPlanProps& props) {
    auto body = conjunct.GetExpressionBody();
    const bool negated = TCoNot::Match(body.Get());
    if (negated) {
        body = body->ChildPtr(0);
    }

    // Only a member of the row argument names an IU.
    if (!TCoMember::Match(body.Get()) || body->HeadPtr().Get() != &conjunct.Node->Head().Head()) {
        return std::nullopt;
    }

    const auto binding = GetMemberId(*body);
    const auto* entry = props.Subplans.Find(binding);
    if (!entry || (entry->Type != ESubplanType::EXISTS && entry->Type != ESubplanType::IN_SUBPLAN)
        || !HasFreeCorrelation(entry->Plan, entry->DependentIUs)) {
        return std::nullopt;
    }
    return TSubplanCall{binding, *entry, negated};
}

// Predicates on the subplan's own columns stay on its input, the rest become join conditions.
bool PullUpFilter(const TOpFilter& filter, const TUnorderedIUs& outerIUs, const TSubstitutions& captures, bool pgSyntax, TPulledUpSubplan& subplan) {
    const auto& innerIUs = subplan.Input->GetOutputIUs();
    auto available = innerIUs;
    available.UnionWith(outerIUs);

    TVector<TExpression> localPredicates;
    for (const auto& conjunct : filter.GetFilterExpression().SplitConjunct()) {
        // Predicates read caller values through local capture IDs. At the join they read the caller's own IDs.
        auto predicate = conjunct.ApplyRenames(captures);
        if (predicate.GetRawInputIUs().IsSubsetOf(innerIUs)) {
            localPredicates.push_back(std::move(predicate));
            continue;
        }
        if (!predicate.GetRawInputIUs().IsSubsetOf(available)) {
            return false;
        }

        if (predicate.MaybeEquiJoinCondition()) {
            // Not a local predicate, so at least one side belongs to the caller.
            TEquiJoinCondition equality(predicate);
            auto outer = equality.GetLeftIU();
            auto inner = equality.GetRightIU();
            if (!outerIUs.Contains(outer)) {
                std::swap(outer, inner);
            }
            if (innerIUs.Contains(inner)) {
                subplan.Keys.Add(outer, inner);
                continue;
            }
        }

        // Caller-only predicates stay in ON as well: a filter above the join
        // would remove rows that NOT EXISTS must preserve.
        subplan.Filters.push_back(std::move(predicate));
    }

    if (!localPredicates.empty()) {
        subplan.Input = MakeIntrusive<TOpFilter>(subplan.Input, filter.Pos, MakeConjunction(localPredicates, pgSyntax));
    }
    return true;
}

bool CanPullUpThrough(const IOperator& op) {
    return op.Kind == EOperator::Filter || op.Kind == EOperator::Map || op.Kind == EOperator::AddDependencies;
}

// The subplan must be a chain of filters, maps and captures over an uncorrelated input: correlation
// anywhere else needs a dependent join. Original operators stay unchanged, so that path can still use them.
std::optional<TPulledUpSubplan> TryPullUpSubplan(const TSubplanEntry& entry, const TUnorderedIUs& outerIUs, TPlanProps& props) {
    TVector<IOperator*> chain;
    TSubstitutions captures;
    auto input = entry.Plan;
    for (; CanPullUpThrough(*input); input = input->GetChild(0)) {
        // Pull-up changes how often these expressions run, so volatile ones stay on the dependent join path.
        if (!CanDuplicateOperator(*input) || !input->GetSubplanIUs(props.Subplans).Empty()) {
            return std::nullopt;
        }
        if (input->Kind != EOperator::AddDependencies) {
            chain.push_back(input.Get());
            continue;
        }

        // The join supplies the captured values, so the rebuilt chain does not need this operator.
        for (const auto& [local, capture] : CastOperator<TOpAddDependencies>(*input).GetDependencies().Items()) {
            if (!outerIUs.Contains(capture.Outer)) {
                return std::nullopt;
            }
            captures.Add(local, capture.Outer);
        }
    }
    if (HasFreeCorrelation(input, entry.DependentIUs)) {
        return std::nullopt;
    }

    TPulledUpSubplan subplan{input, {}, {}};
    for (auto* op : chain | std::views::reverse) {
        if (op->Kind == EOperator::Filter) {
            if (!PullUpFilter(CastOperator<TOpFilter>(*op), outerIUs, captures, props.PgSyntax, subplan)) {
                return std::nullopt;
            }
            continue;
        }

        // Maps stay below the join, where caller columns are not available.
        auto& map = CastOperator<TOpMap>(*op);
        if (!map.GetUsedIUs(props).IsSubsetOf(subplan.Input->GetOutputIUs())) {
            return std::nullopt;
        }
        subplan.Input = MakeIntrusive<TOpMap>(subplan.Input, map.Pos, map.GetMapElements(), map.NeedToPush);
    }
    return subplan;
}

// Join filters run only in the block hash join, which the physical join builder gives up on for some key types.
bool CanUseJoinFilters(const IOperator& left, const IOperator& right, const TJoinIUs& keys, TRBOContext& ctx) {
    return ctx.KqpCtx.Config->GetUseBlockHashJoin() && AllOf(keys.Items(), [&](const auto& key) {
        return IsDataOrOptionalOfData(left.GetIUType(key.first, ctx.ExprCtx)) && IsDataOrOptionalOfData(right.GetIUType(key.second, ctx.ExprCtx));
    });
}

// An anti join keeps a row without a match, but NOT IN is unknown when either side of its
// comparison is null and the subplan is not empty. Without nulls the two are the same.
bool CanUseAntiJoinForNotIn(const TIntrusivePtr<IOperator>& left, const TSubplanEntry& entry, TExprContext& ctx) {
    return !IsNullableIU(left, entry.Tuple.Items()[0], ctx) && !IsNullableIU(entry.Plan, *entry.ResultIU, ctx);
}

TIntrusivePtr<IOperator> TryBuildPulledUpJoin(const TIntrusivePtr<IOperator>& left, const TSubplanCall& call, TPositionHandle pos, TRBOContext& ctx, TPlanProps& props) {
    const auto& entry = call.Entry;
    const bool isIn = entry.Type == ESubplanType::IN_SUBPLAN;
    Y_ENSURE(!isIn || (entry.Tuple.Items().size() == 1 && entry.ResultIU), "IN requires one lookup column and one result");
    if (isIn && call.Negated && !CanUseAntiJoinForNotIn(left, entry, ctx.ExprCtx)) {
        return nullptr;
    }

    auto subplan = TryPullUpSubplan(entry, left->GetOutputIUs(), props);
    if (!subplan) {
        return nullptr;
    }
    if (isIn) {
        subplan->Keys.Add(entry.Tuple.Items()[0], *entry.ResultIU);
    }

    // Semi and anti joins without an equality stay on the dependent join path.
    if (subplan->Keys.Items().empty()) {
        return nullptr;
    }

    // Rebuilt operators have no inferred type yet, but the original subplan has the same IDs.
    if (!subplan->Filters.empty() && !CanUseJoinFilters(*left, *entry.Plan, subplan->Keys, ctx)) {
        return nullptr;
    }
    return MakeIntrusive<TOpJoin>(left, subplan->Input, pos, call.Negated ? "LeftOnly" : "LeftSemi", subplan->Keys, subplan->Filters);
}

} // anonymous namespace

bool TPullUpCorrelatedPredicateRule::QuickMatch(const TIntrusivePtr<IOperator>& input, const TPlanProps& props) const {
    if (input->Kind != EOperator::Filter || props.Subplans.Empty()) {
        return false;
    }

    return AnyOf(input->GetSubplanIUs(props.Subplans), [&](TInfoUnitId iu) {
        const auto type = props.Subplans.At(iu).Type;
        return type == ESubplanType::EXISTS || type == ESubplanType::IN_SUBPLAN;
    });
}

bool TPullUpCorrelatedPredicateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter;
}

TIntrusivePtr<IOperator> TPullUpCorrelatedPredicateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto filter = CastOperator<TOpFilter>(input);
    auto conjuncts = filter->GetFilterExpression().SplitConjunct();

    for (size_t i = 0; i < conjuncts.size(); ++i) {
        const auto call = MatchCorrelatedCall(conjuncts[i], props);
        if (!call) {
            continue;
        }

        // The join consumes the call. If another conjunct needs its value,
        // leave the call to the mark-join path, which materializes that value.
        const bool usedElsewhere = AnyOf(conjuncts, [&](const TExpression& conjunct) {
            return &conjunct != &conjuncts[i] && conjunct.GetRawInputIUs().Contains(call->Binding);
        });
        if (usedElsewhere) {
            continue;
        }

        auto join = TryBuildPulledUpJoin(filter->GetInput(), *call, filter->Pos, ctx, props);
        if (!join) {
            continue;
        }

        props.Subplans.Remove(call->Binding);
        conjuncts.erase(conjuncts.begin() + i);
        if (conjuncts.empty()) {
            return join;
        }
        return MakeIntrusive<TOpFilter>(join, filter->Pos, MakeConjunction(conjuncts, props.PgSyntax));
    }

    return input;
}

} // namespace NKqp
} // namespace NKikimr
