#include "kqp_rules_include.h"

#include "decorrelation/dependent_join_pushdown.h"

namespace NKikimr {
namespace NKqp {

namespace {

// Unlike some, min is deterministic, so a shared plan with the subquery can be copied for each consumer.
TString OnlyValueAggregation(const TTypeAnnotationNode* valueType) {
    if (!valueType) {
        return "some";
    }
    const auto compare = NYql::CanCompare<false>(valueType, valueType);
    return compare == NYql::ECompareOptions::Comparable || compare == NYql::ECompareOptions::Optional ? "min" : "some";
}

// Make sure that scalar subquery produce one row for each binding.
std::pair<TIntrusivePtr<IOperator>, TInfoUnitId> MakeAtMostOneRowPerGroup(const TIntrusivePtr<IOperator>& input, const TOrderedIUs<>& groupKeys,
                                                                          TInfoUnitId valueIU, const TTypeAnnotationNode* valueType,
                                                                          TPositionHandle pos, TRBOContext& ctx, TPlanProps& props) {
    auto rowIU = props.InfoUnitRegistry.AddGenerated("row");
    TMapIUs rowElements;
    rowElements.Add(rowIU, MakeConstant("Uint64", "1", pos, &ctx.ExprCtx));
    auto rowMap = MakeIntrusive<TOpMap>(input, pos, rowElements);

    auto countIU = props.InfoUnitRegistry.AddGenerated("row_count");
    auto valueStateIU = props.InfoUnitRegistry.AddGenerated("scalar_value");

    TAggregationIUs traits;
    traits.Add(countIU, TOpAggregationTraits{rowIU, "count"});
    // This is need to get the actual value, we emit ensure that we get only one row, so its minimum is the value.
    traits.Add(valueStateIU, TOpAggregationTraits{valueIU, OnlyValueAggregation(valueType)});
    auto aggregate = MakeIntrusive<TOpAggregate>(rowMap, traits, groupKeys, EOpPhase::Undefined, /*distinctAll=*/false, pos);

    auto atMostOne =
        MakeBinaryPredicate("<=", MakeColumnAccess(countIU, pos, &ctx.ExprCtx, &props), MakeConstant("Uint64", "1", pos, &ctx.ExprCtx));

    auto checkedIU = props.InfoUnitRegistry.AddGenerated("checked_scalar");
    TMapIUs valueElements;
    // Emit ensure.
    valueElements.Add(checkedIU, MakeEnsure(MakeColumnAccess(valueStateIU, pos, &ctx.ExprCtx, &props), atMostOne,
                                            "Scalar subquery returned more than one row"));
    return std::make_pair(MakeIntrusive<TOpMap>(aggregate, pos, valueElements), checkedIU);
}

} // anonymous namespace

bool TInlineScalarSubplanRule::QuickMatch(const TIntrusivePtr<IOperator>& input, const TPlanProps& props) const {
    if (props.Subplans.Empty()) {
        return false;
    }

    for (const auto iu : input->GetSubplanIUs(props.Subplans)) {
        if (props.Subplans.At(iu).Type == ESubplanType::EXPR) {
            return true;
        }
    }

    return false;
}

// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

bool TInlineScalarSubplanRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    TVector<TInfoUnitId> scalarIUs;
    for (const auto iu : input->GetSubplanIUs(props.Subplans)) {
        if (props.Subplans.At(iu).Type == ESubplanType::EXPR) {
            scalarIUs.push_back(iu);
            break;
        }
    }

    if (scalarIUs.empty()) {
        return false;
    }

    auto scalarIU = scalarIUs[0];
    const auto& subplanEntry = props.Subplans.At(scalarIU);
    auto subplan = CastOperator<IOperator>(subplanEntry.Plan);
    Y_ENSURE(subplanEntry.ResultIU, "Missing scalar result binding");
    auto subplanResIU = *subplanEntry.ResultIU;
    const auto* subplanResType = subplan->Type ? subplan->GetIUType(subplanResIU, ctx.ExprCtx) : nullptr;

    Y_ENSURE(MatchOperator<IUnaryOperator>(input));
    auto unaryOp = CastOperator<IUnaryOperator>(input);

    auto child = unaryOp->GetInput();

    if (HasFreeCorrelation(subplan, subplanEntry.DependentIUs)) {
        auto attachSubplanResult = [&](const TIntrusivePtr<IOperator>& join, TInfoUnitId joinedSubplanResIU) {
            if (input->Kind == EOperator::Filter) {
                auto outerFilter = CastOperator<TOpFilter>(input);
                outerFilter->SetFilterExpression(outerFilter->GetFilterExpression().ApplyRenames({{scalarIU, joinedSubplanResIU}}));
                outerFilter->SetInput(join);
            } else {
                TMapIUs renameElements;
                renameElements.Add(scalarIU, MakeColumnAccess(joinedSubplanResIU, subplan->Pos, &ctx.ExprCtx, &props));
                auto rename = MakeIntrusive<TOpMap>(join, subplan->Pos, renameElements);
                unaryOp->SetInput(rename);
            }
        };

        const auto& dependencies = subplanEntry.DependentIUs;
        auto leftIUs = child->GetOutputIUs();
        for (const auto iu : dependencies) {
            Y_ENSURE(leftIUs.Contains(iu), TStringBuilder() << "Correlation column " << props.InfoUnitRegistry.GetDebugName(iu) << " is not produced by the outer plan");
        }

        // The outer plan and the domain read the child through a Replicate, the domain under fresh IDs.
        auto domain = MakeSubplanDomain(child, dependencies, subplan->Pos, props);
        const TOrderedIUs<> domainColumns(domain.Keys.Right().begin(), domain.Keys.Right().end());
        TJoinIUs joinKeys = domain.Keys;
        auto dependentJoin = std::move(domain).Bind(subplan, subplan->Pos);

        auto [rightInput, rightResIU] = MakeAtMostOneRowPerGroup(dependentJoin, domainColumns, subplanResIU, subplanResType, subplan->Pos, ctx, props);

        TIntrusivePtr<IOperator> joinLeftInput = child;
        TIntrusivePtr<IOperator> joinRightInput = rightInput;
        joinKeys = MakeNullSafeJoinKeys(joinLeftInput, joinRightInput, joinKeys, subplan->Pos, ctx, props);

        auto leftJoin = MakeIntrusive<TOpJoin>(joinLeftInput, joinRightInput, subplan->Pos, "Left", joinKeys);

        attachSubplanResult(leftJoin, rightResIU);
    }
    // Otherwise we assume an uncorrelated supbplan
    else {
        auto [checkedInput, checkedResIU] = MakeAtMostOneRowPerGroup(subplan, {}, subplanResIU, subplanResType, subplan->Pos, ctx, props);

        TMapIUs renameElements;
        renameElements.Add(scalarIU, MakeColumnAccess(checkedResIU, subplan->Pos, &ctx.ExprCtx, &props));
        auto rename = MakeIntrusive<TOpMap>(checkedInput, subplan->Pos, renameElements);

        TJoinIUs joinKeys;
        auto cross = MakeIntrusive<TOpJoin>(child, rename, subplan->Pos, "Cross", joinKeys);
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}
}
}
