#include "kqp_rules_include.h"

#include "decorrelation/dependent_join_pushdown.h"
#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace NKikimr {
namespace NKqp {

namespace {

// Make sure that scalar subquery produce one row for each binding.
std::pair<TIntrusivePtr<IOperator>, TInfoUnit> MakeAtMostOneRowPerGroup(const TIntrusivePtr<IOperator>& input, const TVector<TInfoUnit>& groupKeys,
                                                                        const TInfoUnit& valueIU, TPositionHandle pos, TRBOContext& ctx, TPlanProps& props) {
    TInfoUnitSet usedIUs;
    NMapRenames::AddUsedIUs(usedIUs, input->GetOutputIUs());

    auto rowIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
    TVector<TMapElement> rowElements;
    rowElements.emplace_back(rowIU, MakeConstant("Uint64", "1", pos, &ctx.ExprCtx));
    auto rowMap = MakeIntrusive<TOpMap>(input, pos, rowElements);

    auto countIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
    auto valueStateIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);

    TVector<TOpAggregationTraits> traits;
    traits.emplace_back(rowIU, "count", countIU);
    // This is need to get the actual value, since we have one value we can use min/max.
    traits.emplace_back(valueIU, "min", valueStateIU);
    auto aggregate = MakeIntrusive<TOpAggregate>(rowMap, traits, groupKeys, EOpPhase::Undefined, /*distinctAll=*/false, pos);

    auto atMostOne =
        MakeBinaryPredicate("<=", MakeColumnAccess(countIU, pos, &ctx.ExprCtx, &props), MakeConstant("Uint64", "1", pos, &ctx.ExprCtx));

    auto checkedIU = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
    TVector<TMapElement> valueElements;
    // Emit ensure.
    valueElements.emplace_back(checkedIU, MakeEnsure(MakeColumnAccess(valueStateIU, pos, &ctx.ExprCtx, &props), atMostOne,
                                                     "Scalar subquery returned more than one row"));
    return std::make_pair(MakeIntrusive<TOpMap>(aggregate, pos, valueElements), checkedIU);
}

} // anonymous namespace

// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

bool TInlineScalarSubplanRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    TVector<TInfoUnit> scalarIUs;
    for (const auto& iu : input->GetSubplanIUs(props.Subplans)) {
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
    auto subplanResIU = GetSubplanResultIUs(subplan)[0];

    Y_ENSURE(MatchOperator<IUnaryOperator>(input));
    auto unaryOp = CastOperator<IUnaryOperator>(input);

    auto child = unaryOp->GetInput();

    if (HasFreeCorrelation(subplan, subplanEntry.DependentIUs)) {
        auto attachSubplanResult = [&](const TIntrusivePtr<IOperator>& join, const TInfoUnit& joinedSubplanResIU) {
            if (input->Kind == EOperator::Filter) {
                auto outerFilter = CastOperator<TOpFilter>(input);
                outerFilter->SetFilterExpression(outerFilter->GetFilterExpression().ApplyRenames({{scalarIU, joinedSubplanResIU}}));
                outerFilter->SetInput(join);
            } else {
                TVector<TMapElement> renameElements;
                renameElements.emplace_back(scalarIU, joinedSubplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
                auto rename = MakeIntrusive<TOpMap>(join, subplan->Pos, renameElements);
                unaryOp->SetInput(rename);
            }
        };

        const auto& dependencies = subplanEntry.DependentIUs;
        auto leftIUs = child->GetOutputIUs();
        for (const auto& iu : dependencies) {
            Y_ENSURE(ContainsInfoUnit(leftIUs, iu), TStringBuilder() << "Correlation column " << iu.GetFullName() << " is not produced by the outer plan");
        }

        auto dependentJoin = MakeIntrusive<TOpDependentJoin>(MakeDomainProjection(child, dependencies, subplan->Pos), subplan, dependencies, subplan->Pos);

        auto [rightInput, rightResIU] = MakeAtMostOneRowPerGroup(dependentJoin, dependencies, subplanResIU, subplan->Pos, ctx, props);
        auto rightIUs = rightInput->GetOutputIUs();
        THashSet<TInfoUnit, TInfoUnit::THashFunction> usedIUs;
        NMapRenames::AddUsedIUs(usedIUs, leftIUs);
        NMapRenames::AddUsedIUs(usedIUs, rightIUs);

        NMapRenames::TRenameMap subplanOutputRenames;
        for (const auto& iu : rightIUs) {
            if (ContainsInfoUnit(leftIUs, iu) && !subplanOutputRenames.contains(iu)) {
                subplanOutputRenames.emplace(iu, NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs));
            }
        }

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        for (const auto& iu : dependencies) {
            joinKeys.push_back(std::make_pair(iu, iu));
        }
        TIntrusivePtr<IOperator> joinLeftInput = child;
        TIntrusivePtr<IOperator> joinRightInput = rightInput;
        joinKeys = MakeNullSafeJoinKeys(joinLeftInput, joinRightInput, joinKeys, subplan->Pos, ctx, props, usedIUs);

        auto joinedSubplanResIU = rightResIU;
        if (const auto renameIt = subplanOutputRenames.find(joinedSubplanResIU); renameIt != subplanOutputRenames.end()) {
            joinedSubplanResIU = renameIt->second;
        }

        auto leftJoin = NMapRenames::MakeJoinWithRightRenames(joinLeftInput, joinRightInput, subplan->Pos, "Left", joinKeys, {},
                                                              subplanOutputRenames, ctx.ExprCtx, props);

        attachSubplanResult(leftJoin, joinedSubplanResIU);
    }
    // Otherwise we assume an uncorrelated supbplan
    else {
        auto [checkedInput, checkedResIU] = MakeAtMostOneRowPerGroup(subplan, {}, subplanResIU, subplan->Pos, ctx, props);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, checkedResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = MakeIntrusive<TOpMap>(checkedInput, subplan->Pos, renameElements);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        auto cross = MakeIntrusive<TOpJoin>(child, rename, subplan->Pos, "Cross", joinKeys);
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}
}
}
