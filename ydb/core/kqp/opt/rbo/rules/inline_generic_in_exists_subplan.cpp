#include "kqp_rules_include.h"

#include "decorrelation/dependent_join_pushdown.h"
#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace {

using namespace NKikimr::NKqp;
using namespace NKikimr::NKqp::NMapRenames;

// The null of the Bool type, which the three valued result of an IN needs as a value and not only
// as the absence of a row.
TExprNode::TPtr MakeNullBoolNode(TPositionHandle pos, TExprContext& ctx) {
    auto boolType = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Bool")});
    return ctx.NewCallable(pos, "Nothing", {ctx.NewCallable(pos, "OptionalType", {boolType})});
}

void AddDomainColumn(TVector<TInfoUnit>& domain, const TInfoUnit& iu) {
    if (!ContainsInfoUnit(domain, iu)) {
        domain.push_back(iu);
    }
}

}

namespace NKikimr {
namespace NKqp {

bool TInlineGenericInExistsSubplanRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter;
}

TIntrusivePtr<IOperator> TInlineGenericInExistsSubplanRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter) {
        return input;
    }

    // Check that the filter lambda contains at least one in/exists subplan
    auto filter = CastOperator<TOpFilter>(input);
    TVector<TInfoUnit> inOrExistsSubplans;

    for (const auto& subplanIU : filter->GetSubplanIUs(props.Subplans)) {
        const auto type = props.Subplans.At(subplanIU).Type;
        if (type == ESubplanType::IN_SUBPLAN || type == ESubplanType::EXISTS) {
            inOrExistsSubplans.push_back(subplanIU);
        }
    }

    if (inOrExistsSubplans.empty()) {
        return input;
    }

    // Now we will pick the first subplan IU and join its subplan before filter
    // Then we'll remove the subplan from subplans list and rebuild the filter expression
    // so the current iu is no longer marked as SubplanIU

    auto subplanIU = inOrExistsSubplans[0];
    auto subplanEntry = props.Subplans.At(subplanIU);
    TIntrusivePtr<IOperator> newFilterInput;
    auto subplan = CastOperator<IOperator>(subplanEntry.Plan);

    const bool useDependentJoin = HasFreeCorrelation(subplan, subplanEntry.DependentIUs);
    if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || useDependentJoin) {
        TIntrusivePtr<IOperator> leftInput = filter->GetInput();
        auto rightInput = subplan;
        const auto outerIUs = leftInput->GetOutputIUs();
        const auto originalPlanIUs = useDependentJoin ? GetSubplanResultIUs(rightInput) : rightInput->GetOutputIUs();

        TVector<TInfoUnit> domain;
        if (useDependentJoin) {
            for (const auto& iu : subplanEntry.DependentIUs) {
                AddDomainColumn(domain, iu);
            }
        } else {
            for (const auto& iu : subplanEntry.Tuple) {
                AddDomainColumn(domain, iu);
            }
        }

        Y_ENSURE(!domain.empty(), "Cannot decorrelate in/exists subplan without correlated columns");
        for (const auto& iu : domain) {
            Y_ENSURE(ContainsInfoUnit(outerIUs, iu),
                     TStringBuilder() << "Correlation column " << iu.GetFullName() << " is not produced by the outer plan");
        }

        // For exists we can emulate a mkrk join with 2 values output(true, false).
        bool markMissingAsFalse = subplanEntry.Type == ESubplanType::EXISTS;
        if (!markMissingAsFalse) {
            markMissingAsFalse = true;
            for (size_t i = 0; i < subplanEntry.Tuple.size() && markMissingAsFalse; i++) {
                markMissingAsFalse = !IsNullableIU(leftInput, subplanEntry.Tuple[i]) && !IsNullableIU(rightInput, originalPlanIUs[i]);
            }
        }

        Y_ENSURE(subplanEntry.Type == ESubplanType::EXISTS || (subplanEntry.Type == ESubplanType::IN_SUBPLAN && subplanEntry.Tuple.size() == 1));
        // For in we have to emulate three value result (true, false, null).
        const bool threeValued = !markMissingAsFalse && subplanEntry.Type == ESubplanType::IN_SUBPLAN && subplanEntry.Tuple.size() == 1;

        TInfoUnitSet usedIUs;
        AddUsedIUs(usedIUs, outerIUs);
        AddUsedIUs(usedIUs, originalPlanIUs);

        TVector<TInfoUnit> markColumns = domain;
        TVector<std::pair<TInfoUnit, TInfoUnit>> domainJoinKeys;
        TVector<std::pair<TInfoUnit, TInfoUnit>> tupleJoinKeys;
        for (const auto& iu : domain) {
            domainJoinKeys.push_back(std::make_pair(iu, iu));
        }

        TIntrusivePtr<IOperator> statsSource;
        TVector<TInfoUnit> statsKeys;
        TInfoUnit compareResultIU;

        TIntrusivePtr<IOperator> matchSource;
        if (useDependentJoin) {
            matchSource = MakeIntrusive<TOpDependentJoin>(MakeDomainProjection(leftInput, domain, filter->Pos), rightInput, domain, filter->Pos);

            for (size_t i = 0; i < subplanEntry.Tuple.size(); i++) {
                AddDomainColumn(markColumns, originalPlanIUs[i]);
                tupleJoinKeys.push_back(std::make_pair(subplanEntry.Tuple[i], originalPlanIUs[i]));
            }

            statsKeys = domain;
            if (threeValued) {
                compareResultIU = originalPlanIUs[0];
            }
        } else {
            const auto commonIUs = IUSetIntersect(domain, originalPlanIUs);
            const auto rightRenamings = MakeRenameMap(commonIUs, props.InternalVarIdx, usedIUs);
            if (!rightRenamings.empty()) {
                rightInput = MakeMapFromRenames(rightInput, rightRenamings, filter->Pos, ctx.ExprCtx, props);
            }

            TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
            auto planIUs = rightInput->GetOutputIUs();
            for (size_t i = 0; i < subplanEntry.Tuple.size(); i++) {
                joinKeys.push_back(std::make_pair(subplanEntry.Tuple[i], planIUs[i]));
            }
            matchSource = MakeIntrusive<TOpJoin>(MakeDomainProjection(leftInput, domain, filter->Pos), rightInput, input->Pos, "Inner", joinKeys);

            statsSource = rightInput;
            if (threeValued) {
                compareResultIU = planIUs[0];
            }
        }

        auto matchedDomain = MakeDomainProjection(matchSource, markColumns, filter->Pos);
        if (!statsSource) {
            statsSource = matchedDomain;
        }

        // Here we want to emulate a mark join, rewriting it into:
        // coalesce(leftjoin(left input, map(true, (dependent join(...)), false).
        // So as result we will get true for columns which survive dependent join and false for rest.
        auto markIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
        TVector<TMapElement> markElements;
        markElements.emplace_back(markIU, MakeConstant("Bool", "true", filter->Pos, &ctx.ExprCtx));
        auto markMap = MakeIntrusive<TOpMap>(matchedDomain, filter->Pos, markElements);
        const auto topRenamings = MakeRenameMap(markColumns, props.InternalVarIdx, usedIUs);
        TIntrusivePtr<IOperator> markRight = markMap;
        auto markJoinKeys = useDependentJoin ? MakeNullSafeJoinKeys(leftInput, markRight, domainJoinKeys, filter->Pos, ctx, props, usedIUs) : domainJoinKeys;
        markJoinKeys.insert(markJoinKeys.end(), tupleJoinKeys.begin(), tupleJoinKeys.end());

        TIntrusivePtr<IOperator> markJoin =
            NMapRenames::MakeJoinWithRightRenames(leftInput, markRight, filter->Pos, "Left", markJoinKeys, {}, topRenamings, ctx.ExprCtx, props);

        auto column = [&](const TInfoUnit& iu) { return MakeColumnAccess(iu, filter->Pos, &ctx.ExprCtx, &props); };
        auto falseConst = MakeConstant("Bool", "false", filter->Pos, &ctx.ExprCtx);
        auto matched = MakeBinaryPredicate("Coalesce", column(markIU), falseConst);

        TIntrusivePtr<IOperator> resultInput = markJoin;
        TVector<TMapElement> resultElements;

        if (markMissingAsFalse) {
            resultElements.emplace_back(subplanIU, matched);
        } else if (threeValued) {
            // This one is an attempt to emulate three value result. For projection column we need to know does it contain null or not for each binding. So we have a special
            // pipeline with aggregation lets call it statistics. We will count(1) as num_rows, count(projection column) as num_rows_not_null group by domain columns. 
            // Has null if (num_rows > num_rows_not_null).
            auto rowIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
            TVector<TMapElement> rowElements;
            rowElements.emplace_back(rowIU, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx));
            auto rowMap = MakeIntrusive<TOpMap>(statsSource, filter->Pos, rowElements);

            auto valueCountIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
            auto rowCountIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);

            // count(projection), count(1)
            TVector<TOpAggregationTraits> statsTraits;
            statsTraits.emplace_back(compareResultIU, "count", valueCountIU);
            statsTraits.emplace_back(rowIU, "count", rowCountIU);
            auto statsAggregate = MakeIntrusive<TOpAggregate>(rowMap, statsTraits, statsKeys, EOpPhase::Undefined, /*distinctAll=*/false, filter->Pos);

            auto hasNullIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
            auto nonEmptyIU = MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
            TVector<TMapElement> statsElements;
            // Does it have null columns?.
            statsElements.emplace_back(hasNullIU, MakeBinaryPredicate(">", column(rowCountIU), column(valueCountIU)));
            if (statsKeys.empty()) {
                statsElements.emplace_back(nonEmptyIU, MakeBinaryPredicate(">", column(rowCountIU), MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx)));
            } else {
                // Always has some rows.
                statsElements.emplace_back(nonEmptyIU, MakeConstant("Bool", "true", filter->Pos, &ctx.ExprCtx));
            }
            auto statsMap = MakeIntrusive<TOpMap>(statsAggregate, filter->Pos, statsElements);

            TVector<std::pair<TInfoUnit, TInfoUnit>> statsJoinKeys;
            for (const auto& iu : statsKeys) {
                statsJoinKeys.push_back(std::make_pair(iu, iu));
            }
            const auto statsRenamings = MakeRenameMap(statsKeys, props.InternalVarIdx, usedIUs);

            TIntrusivePtr<IOperator> statsLeft = markJoin;
            TIntrusivePtr<IOperator> statsRight = statsMap;
            statsJoinKeys = MakeNullSafeJoinKeys(statsLeft, statsRight, statsJoinKeys, filter->Pos, ctx, props, usedIUs);

            resultInput = MakeJoinWithRightRenames(statsLeft, statsRight, filter->Pos, statsKeys.empty() ? "Cross" : "Left", statsJoinKeys, {}, statsRenamings,
                                                   ctx.ExprCtx, props);

            TVector<TExpression> unknownTerms;
            unknownTerms.push_back(MakeBinaryPredicate("Coalesce", column(hasNullIU), falseConst));

            // This emulates a three value semantis if lookup column is null.
            if (IsNullableIU(leftInput, subplanEntry.Tuple[0])) {
                auto lookupColumn = column(subplanEntry.Tuple[0]);
                auto lookupIsNull = MakeNegation(MakeBinaryPredicate("Coalesce", MakeBinaryPredicate("==", lookupColumn, lookupColumn), falseConst));
                unknownTerms.push_back(MakeBinaryPredicate("And", lookupIsNull, MakeBinaryPredicate("Coalesce", column(nonEmptyIU), falseConst)));
            }

            auto unknown = unknownTerms[0];
            for (size_t i = 1; i < unknownTerms.size(); i++) {
                unknown = MakeBinaryPredicate("Or", unknown, unknownTerms[i]);
            }

            auto nullBool = TExpression(MakeNullBoolNode(filter->Pos, ctx.ExprCtx), &ctx.ExprCtx, &props);
            resultElements.emplace_back(subplanIU, MakeBinaryPredicate("Or", matched, MakeBinaryPredicate("And", unknown, nullBool)));
        } else {
            resultElements.emplace_back(subplanIU, markIU, filter->Pos, &ctx.ExprCtx, &props);
        }

        newFilterInput = MakeIntrusive<TOpMap>(resultInput, filter->Pos, resultElements);
    }
    // uncorrelated EXISTS
    else {
        auto zero = MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx);
        auto limit = MakeIntrusive<TOpLimit>(subplan, filter->Pos, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx), EOpPhase::Undefined);

        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;
        countMapElements.emplace_back(countResult, zero);
        auto countMap = MakeIntrusive<TOpMap>(limit, filter->Pos, countMapElements);

        TOpAggregationTraits aggFunction(countResult, "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = MakeIntrusive<TOpAggregate>(countMap, aggs, keyColumns, EOpPhase::Final, false, filter->Pos);

        auto comparePredicate = MakeBinaryPredicate("!=", MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        mapElements.emplace_back(subplanIU, comparePredicate);

        auto map = MakeIntrusive<TOpMap>(agg, filter->Pos, mapElements);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        newFilterInput = MakeIntrusive<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);
    }

    props.Subplans.Remove(subplanIU);

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    return MakeIntrusive<TOpFilter>(newFilterInput, filter->Pos, TExpression(filter->GetFilterExpression().GetLambda(), &ctx.ExprCtx, &props));
}
}
}
