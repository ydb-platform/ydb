#include "kqp_rules_include.h"

#include "decorrelation/dependent_join_pushdown.h"

namespace {

using namespace NKikimr::NKqp;

// The null of the Bool type, which the three valued result of an IN needs as a value and not only
// as the absence of a row.
TExprNode::TPtr MakeNullBoolNode(TPositionHandle pos, TExprContext& ctx) {
    auto boolType = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Bool")});
    return ctx.NewCallable(pos, "Nothing", {ctx.NewCallable(pos, "OptionalType", {boolType})});
}

void AddDomainColumn(TUnorderedIUs& domain, TInfoUnitId iu) {
    if (!domain.Contains(iu)) {
        domain.Add(iu);
    }
}

}

namespace NKikimr {
namespace NKqp {

bool TInlineGenericInExistsSubplanRule::QuickMatch(const TIntrusivePtr<IOperator>& input, const TPlanProps& props) const {
    if (input->Kind != EOperator::Filter || props.Subplans.Empty()) {
        return false;
    }

    for (const auto& iu : input->GetSubplanIUs(props.Subplans)) {
        const auto type = props.Subplans.At(iu).Type;
        if (type == ESubplanType::IN_SUBPLAN || type == ESubplanType::EXISTS) {
            return true;
        }
    }

    return false;
}

bool TInlineGenericInExistsSubplanRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter;
}

TIntrusivePtr<IOperator> TInlineGenericInExistsSubplanRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter) {
        return input;
    }

    // Check that the filter lambda contains at least one in/exists subplan
    auto filter = CastOperator<TOpFilter>(input);
    TVector<TInfoUnitId> inOrExistsSubplans;

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
    const auto& subplanEntry = props.Subplans.At(subplanIU);
    TIntrusivePtr<IOperator> newFilterInput;
    auto subplan = CastOperator<IOperator>(subplanEntry.Plan);

    const bool useDependentJoin = HasFreeCorrelation(subplan, subplanEntry.DependentIUs);
    if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || useDependentJoin) {
        TIntrusivePtr<IOperator> leftInput = filter->GetInput();
        auto rightInput = subplan;
        const auto outerIUs = leftInput->GetOutputIUs();
        TVector<TInfoUnitId> originalPlanIUs;
        if (subplanEntry.ResultIU) {
            originalPlanIUs.push_back(*subplanEntry.ResultIU);
        }

        TUnorderedIUs domain;
        if (useDependentJoin) {
            for (const auto& iu : subplanEntry.DependentIUs) {
                AddDomainColumn(domain, iu);
            }
        } else {
            for (const auto& iu : subplanEntry.Tuple.Items()) {
                AddDomainColumn(domain, iu);
            }
        }

        Y_ENSURE(!domain.Empty(), "Cannot decorrelate in/exists subplan without correlated columns");
        for (const auto& iu : domain) {
            Y_ENSURE(outerIUs.Contains(iu),
                     TStringBuilder() << "Correlation column " << props.InfoUnitRegistry.GetDebugName(iu) << " is not produced by the outer plan");
        }

        // For exists we can emulate a mkrk join with 2 values output(true, false).
        bool markMissingAsFalse = subplanEntry.Type == ESubplanType::EXISTS;
        if (!markMissingAsFalse) {
            markMissingAsFalse = true;
            for (size_t i = 0; i < subplanEntry.Tuple.Items().size() && markMissingAsFalse; i++) {
                markMissingAsFalse = !IsNullableIU(leftInput, subplanEntry.Tuple.Items()[i], ctx.ExprCtx) && !IsNullableIU(rightInput, originalPlanIUs[i], ctx.ExprCtx);
            }
        }

        Y_ENSURE(subplanEntry.Type == ESubplanType::EXISTS || (subplanEntry.Type == ESubplanType::IN_SUBPLAN && subplanEntry.Tuple.Items().size() == 1 && subplanEntry.ResultIU));
        // For in we have to emulate three value result (true, false, null).
        const bool threeValued = !markMissingAsFalse && subplanEntry.Type == ESubplanType::IN_SUBPLAN && subplanEntry.Tuple.Items().size() == 1;

        // The mark join and the domain read leftInput through a Replicate, the domain under fresh IDs.
        auto subplanDomain = MakeSubplanDomain(leftInput, domain, filter->Pos, props);
        const TSubstitutions domainColumns(subplanDomain.Keys.Items().begin(), subplanDomain.Keys.Items().end());

        TUnorderedIUs markColumns = subplanDomain.Keys.Right();
        TJoinIUs domainJoinKeys = subplanDomain.Keys;
        TJoinIUs tupleJoinKeys;

        TIntrusivePtr<IOperator> statsSource;
        TUnorderedIUs statsKeys;
        TJoinIUs statsJoinKeys;
        TInfoUnitId compareResultIU = 0;

        TIntrusivePtr<IOperator> matchSource;
        if (useDependentJoin) {
            matchSource = std::move(subplanDomain).Bind(rightInput, filter->Pos);

            for (size_t i = 0; i < subplanEntry.Tuple.Items().size(); i++) {
                AddDomainColumn(markColumns, originalPlanIUs[i]);
                tupleJoinKeys.Add(subplanEntry.Tuple.Items()[i], originalPlanIUs[i]);
            }

            if (threeValued) {
                compareResultIU = originalPlanIUs[0];
            }
        } else {
            // The match join and the statistics read the subplan through a Replicate.
            if (threeValued) {
                auto hub = TReplicate::Create(rightInput, filter->Pos, props.InfoUnitRegistry);
                rightInput = hub->AddOutput();
                statsSource = hub->AddOutput();
            }

            TJoinIUs joinKeys;
            const auto& planIUs = originalPlanIUs;
            for (size_t i = 0; i < subplanEntry.Tuple.Items().size(); i++) {
                joinKeys.Add(domainColumns.At(subplanEntry.Tuple.Items()[i]), planIUs[i]);
            }
            matchSource = MakeIntrusive<TOpJoin>(subplanDomain.Input, rightInput, input->Pos, "Inner", joinKeys);

            if (threeValued) {
                compareResultIU = CastOperator<TOpReplicate>(statsSource)->GetRebindings().At(planIUs[0]);
            }
        }

        TIntrusivePtr<IOperator> matchedDomain = MakeDomainProjection(matchSource, markColumns, filter->Pos);
        if (!statsSource && threeValued) {
            // The mark join and the statistics read the matched domain through a Replicate.
            auto hub = TReplicate::Create(matchedDomain, filter->Pos, props.InfoUnitRegistry);
            matchedDomain = hub->AddOutput();
            auto stats = hub->AddOutput();
            compareResultIU = stats->GetRebindings().At(compareResultIU);
            for (const auto& key : domainJoinKeys.Items()) {
                statsKeys.Add(stats->GetRebindings().At(key.second));
                statsJoinKeys.Add(key.first, stats->GetRebindings().At(key.second));
            }
            statsSource = stats;
        }

        // Here we want to emulate a mark join, rewriting it into:
        // coalesce(leftjoin(left input, map(true, (dependent join(...)), false).
        // So as result we will get true for columns which survive dependent join and false for rest.
        auto markIU = props.InfoUnitRegistry.AddGenerated("in_mark");
        TMapIUs markElements;
        markElements.Add(markIU, MakeConstant("Bool", "true", filter->Pos, &ctx.ExprCtx));
        auto markMap = MakeIntrusive<TOpMap>(matchedDomain, filter->Pos, markElements);
        TIntrusivePtr<IOperator> markRight = markMap;
        auto markJoinKeys = useDependentJoin ? MakeNullSafeJoinKeys(leftInput, markRight, domainJoinKeys, filter->Pos, ctx, props) : domainJoinKeys;
        for (const auto& key : tupleJoinKeys.Items()) {
            markJoinKeys.Add(key);
        }

        TIntrusivePtr<IOperator> markJoin =
            MakeIntrusive<TOpJoin>(leftInput, markRight, filter->Pos, "Left", markJoinKeys);

        auto column = [&](TInfoUnitId iu) { return MakeColumnAccess(iu, filter->Pos, &ctx.ExprCtx, &props); };
        auto falseConst = MakeConstant("Bool", "false", filter->Pos, &ctx.ExprCtx);
        auto matched = MakeBinaryPredicate("Coalesce", column(markIU), falseConst);

        TIntrusivePtr<IOperator> resultInput = markJoin;
        TMapIUs resultElements;

        if (markMissingAsFalse) {
            resultElements.Add(subplanIU, matched);
        } else if (threeValued) {
            // This one is an attempt to emulate three value result. For projection column we need to know does it contain null or not for each binding. So we have a special
            // pipeline with aggregation lets call it statistics. We will count(1) as num_rows, count(projection column) as num_rows_not_null group by domain columns. 
            // Has null if (num_rows > num_rows_not_null).
            auto rowIU = props.InfoUnitRegistry.AddGenerated("row");
            TMapIUs rowElements;
            rowElements.Add(rowIU, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx));
            auto rowMap = MakeIntrusive<TOpMap>(statsSource, filter->Pos, rowElements);

            auto valueCountIU = props.InfoUnitRegistry.AddGenerated("value_count");
            auto rowCountIU = props.InfoUnitRegistry.AddGenerated("row_count");

            // count(projection), count(1)
            TAggregationIUs statsTraits;
            statsTraits.Add(valueCountIU, TOpAggregationTraits{compareResultIU, "count"});
            statsTraits.Add(rowCountIU, TOpAggregationTraits{rowIU, "count"});
            auto statsAggregate = MakeIntrusive<TOpAggregate>(rowMap, statsTraits, TOrderedIUs<>(statsKeys.begin(), statsKeys.end()), EOpPhase::Undefined, /*distinctAll=*/false, filter->Pos);

            auto hasNullIU = props.InfoUnitRegistry.AddGenerated("in_has_null");
            auto nonEmptyIU = props.InfoUnitRegistry.AddGenerated("in_non_empty");
            TMapIUs statsElements;
            // Does it have null columns?.
            statsElements.Add(hasNullIU, MakeBinaryPredicate(">", column(rowCountIU), column(valueCountIU)));
            if (statsKeys.Empty()) {
                statsElements.Add(nonEmptyIU, MakeBinaryPredicate(">", column(rowCountIU), MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx)));
            } else {
                // Always has some rows.
                statsElements.Add(nonEmptyIU, MakeConstant("Bool", "true", filter->Pos, &ctx.ExprCtx));
            }
            auto statsMap = MakeIntrusive<TOpMap>(statsAggregate, filter->Pos, statsElements);

            TIntrusivePtr<IOperator> statsLeft = markJoin;
            TIntrusivePtr<IOperator> statsRight = statsMap;
            statsJoinKeys = MakeNullSafeJoinKeys(statsLeft, statsRight, statsJoinKeys, filter->Pos, ctx, props);

            resultInput = MakeIntrusive<TOpJoin>(statsLeft, statsRight, filter->Pos, statsKeys.Empty() ? "Cross" : "Left", statsJoinKeys);

            TVector<TExpression> unknownTerms;
            unknownTerms.push_back(MakeBinaryPredicate("Coalesce", column(hasNullIU), falseConst));

            // This emulates a three value semantis if lookup column is null.
            if (IsNullableIU(leftInput, subplanEntry.Tuple.Items()[0], ctx.ExprCtx)) {
                auto lookupColumn = column(subplanEntry.Tuple.Items()[0]);
                auto lookupIsNull = MakeNegation(MakeBinaryPredicate("Coalesce", MakeBinaryPredicate("==", lookupColumn, lookupColumn), falseConst));
                unknownTerms.push_back(MakeBinaryPredicate("And", lookupIsNull, MakeBinaryPredicate("Coalesce", column(nonEmptyIU), falseConst)));
            }

            auto unknown = unknownTerms[0];
            for (size_t i = 1; i < unknownTerms.size(); i++) {
                unknown = MakeBinaryPredicate("Or", unknown, unknownTerms[i]);
            }

            auto nullBool = TExpression(MakeNullBoolNode(filter->Pos, ctx.ExprCtx), &ctx.ExprCtx, &props);
            resultElements.Add(subplanIU, MakeBinaryPredicate("Or", matched, MakeBinaryPredicate("And", unknown, nullBool)));
        } else {
            resultElements.Add(subplanIU, column(markIU));
        }

        newFilterInput = MakeIntrusive<TOpMap>(resultInput, filter->Pos, resultElements);
    }
    // uncorrelated EXISTS
    else {
        auto zero = MakeConstant("Uint64", "0", filter->Pos, &ctx.ExprCtx);
        auto limit = MakeIntrusive<TOpLimit>(subplan, filter->Pos, MakeConstant("Uint64", "1", filter->Pos, &ctx.ExprCtx), EOpPhase::Undefined);

        // The counted rows and their count are distinct IUs.
        auto countInput = props.InfoUnitRegistry.AddGenerated("row");
        auto countResult = props.InfoUnitRegistry.AddGenerated("row_count");
        TMapIUs countMapElements;
        countMapElements.Add(countInput, zero);
        auto countMap = MakeIntrusive<TOpMap>(limit, filter->Pos, countMapElements);

        TAggregationIUs aggs;
        aggs.Add(countResult, TOpAggregationTraits{countInput, "count"});
        TOrderedIUs<> keyColumns;

        auto agg = MakeIntrusive<TOpAggregate>(countMap, aggs, keyColumns, EOpPhase::Final, false, filter->Pos);

        auto comparePredicate = MakeBinaryPredicate("!=", MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TMapIUs mapElements;
        mapElements.Add(subplanIU, comparePredicate);

        auto map = MakeIntrusive<TOpMap>(agg, filter->Pos, mapElements);

        TJoinIUs joinKeys;
        newFilterInput = MakeIntrusive<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);
    }

    props.Subplans.Remove(subplanIU);

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    return MakeIntrusive<TOpFilter>(newFilterInput, filter->Pos, TExpression(filter->GetFilterExpression().GetLambda(), &ctx.ExprCtx, &props));
}
}
}
