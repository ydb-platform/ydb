#include "kqp_rules_include.h"

#include "decorrelation/dependent_join_pushdown.h"
#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace NKikimr {
namespace NKqp {

bool TInlineSimpleInExistsSubplanRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter;
}

TIntrusivePtr<IOperator> TInlineSimpleInExistsSubplanRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter || props.PgSyntax) {
        return input;
    }

    // Check that the filter lambda is a conjunction of one or more elements
    auto filter = CastOperator<TOpFilter>(input);
    auto lambdaBody = filter->GetFilterExpression().Node->ChildPtr(1);

    if (!TCoAnd::Match(lambdaBody.Get()) && !TCoNot::Match(lambdaBody.Get()) && !TCoMember::Match(lambdaBody.Get())) {
        return input;
    }

    // Decompose the conjunction into individual conjuncts
    auto conjuncts = filter->GetFilterExpression().SplitConjunct();

    // Find the first conjunct that is a simple in or exists subplan
    bool negated = false;
    TInfoUnit iu;
    const TSubplanEntry* subplanEntry = nullptr;
    size_t conjunctIdx;

    for (conjunctIdx = 0; conjunctIdx < conjuncts.size(); conjunctIdx++) {
        auto maybeSubplan = conjuncts[conjunctIdx].GetExpressionBody();

        bool conjunctNegated = false;
        if (TCoNot::Match(maybeSubplan.Get())) {
            maybeSubplan = maybeSubplan->ChildPtr(0);
            conjunctNegated = true;
        }
        if (TCoMember::Match(maybeSubplan.Get())) {
            auto name = TString(maybeSubplan->ChildPtr(1)->Content());
            iu = TInfoUnit(name);
            if (const auto* entry = props.Subplans.Find(iu)) {
                if (entry->Type == ESubplanType::IN_SUBPLAN || entry->Type == ESubplanType::EXISTS) {
                    subplanEntry = entry;
                    negated = conjunctNegated;
                    break;
                }
            }
        }
    }

    if (conjunctIdx == conjuncts.size()) {
        return input;
    }

    TIntrusivePtr<IOperator> join;
    Y_ENSURE(subplanEntry);
    auto subplan = CastOperator<IOperator>(subplanEntry->Plan);
    const bool useDependentJoin = HasFreeCorrelation(subplan, subplanEntry->DependentIUs);

    // If `NOT` and optional column the result could be nothing.
    if (negated && subplanEntry->Type == ESubplanType::IN_SUBPLAN && subplanEntry->Tuple.size() == 1) {
        const auto& leftInput = filter->GetInput();
        const auto subplanIUs = useDependentJoin ? GetSubplanResultIUs(subplan) : subplan->GetOutputIUs();
        if (subplanIUs.empty() || IsNullableIU(leftInput, subplanEntry->Tuple[0]) || IsNullableIU(subplan, subplanIUs[0])) {
            return input;
        }
    }

    // Simple rewrite into left only/ left semi.
    if (subplanEntry->Type == ESubplanType::IN_SUBPLAN || useDependentJoin) {
        TIntrusivePtr<IOperator> leftJoinInput = filter->GetInput();
        auto joinKind = negated ? "LeftOnly" : "LeftSemi";
        TVector<std::pair<TInfoUnit, TInfoUnit>> tupleJoinKeys;

        auto planIUs = GetSubplanResultIUs(subplan);

        for (size_t i = 0; i < subplanEntry->Tuple.size(); i++) {
            tupleJoinKeys.push_back(std::make_pair(subplanEntry->Tuple[i], planIUs[i]));
        }

        if (useDependentJoin) {
            const auto outerIUs = leftJoinInput->GetOutputIUs();
            for (const auto& dependency : subplanEntry->DependentIUs) {
                Y_ENSURE(ContainsInfoUnit(outerIUs, dependency),
                         TStringBuilder() << "Correlation column " << dependency.GetFullName() << " is not produced by the outer plan");
            }

            // (Domain dependent join leftInput)
            TIntrusivePtr<IOperator> rightInput = MakeIntrusive<TOpDependentJoin>(MakeDomainProjection(leftJoinInput, subplanEntry->DependentIUs, filter->Pos),
                                                                                  subplan, subplanEntry->DependentIUs, filter->Pos);
            TInfoUnitSet usedIUs;
            NMapRenames::AddUsedIUs(usedIUs, outerIUs);
            NMapRenames::AddUsedIUs(usedIUs, rightInput->GetOutputIUs());

            NMapRenames::TRenameMap rightRenames;
            for (const auto& rightIU : rightInput->GetOutputIUs()) {
                if (ContainsInfoUnit(outerIUs, rightIU) && !rightRenames.contains(rightIU)) {
                    rightRenames.emplace(rightIU, NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs));
                }
            }

            TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
            // Add domain keys for join keys.
            for (const auto& dependency : subplanEntry->DependentIUs) {
                joinKeys.push_back(std::make_pair(dependency, dependency));
            }
            joinKeys = MakeNullSafeJoinKeys(leftJoinInput, rightInput, joinKeys, filter->Pos, ctx, props, usedIUs);
            joinKeys.insert(joinKeys.end(), tupleJoinKeys.begin(), tupleJoinKeys.end());

            join = NMapRenames::MakeJoinWithRightRenames(leftJoinInput, rightInput, input->Pos, joinKind, joinKeys, {}, rightRenames, ctx.ExprCtx, props);
        } else {
            join = MakeIntrusive<TOpJoin>(leftJoinInput, subplan, input->Pos, joinKind, tupleJoinKeys);
        }

        conjuncts.erase(conjuncts.begin() + conjunctIdx);
    }
    // EXISTS and NOT EXISTS
    else {
        auto limit = MakeIntrusive<TOpLimit>(subplan, filter->Pos, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx), EOpPhase::Undefined);

        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;
        auto zero = MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx);
        countMapElements.emplace_back(countResult, zero);
        auto countMap = MakeIntrusive<TOpMap>(limit, filter->Pos, countMapElements);

        TOpAggregationTraits aggFunction(countResult, "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = MakeIntrusive<TOpAggregate>(countMap, aggs, keyColumns, EOpPhase::Final, false, filter->Pos);
        const TString compareCallable = negated ? "==" : "!=";

        auto comparePredicate = MakeBinaryPredicate(compareCallable, MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        auto compareResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        mapElements.emplace_back(compareResult, comparePredicate);
        auto map = MakeIntrusive<TOpMap>(agg, filter->Pos, mapElements);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        join = MakeIntrusive<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);

        conjuncts[conjunctIdx] = MakeColumnAccess(compareResult, filter->Pos, &ctx.ExprCtx, &props);
    }

    props.Subplans.Remove(iu);
    // If there was a single conjunct, we can get rid of the filter completely
    if (conjuncts.empty()) {
        return join;
    }

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    return MakeIntrusive<TOpFilter>(join, filter->Pos, MakeConjunction(conjuncts, props.PgSyntax));
}
}
}
