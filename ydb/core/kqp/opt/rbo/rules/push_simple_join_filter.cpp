#include "kqp_rules_include.h"


namespace NKikimr {
namespace NKqp {

bool TPushSimpleJoinFilterRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

bool TPushSimpleJoinFilterRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Join) {
        return false;
    }

    auto join = CastOperator<TOpJoin>(input);
    if (join->JoinFilters.empty())
    {
        return false;
    }

    if (join->JoinKind != "Inner" && join->JoinKind != "Cross" && join->JoinKind != "Left" && join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly") {
        YQL_CLOG(TRACE, CoreDq) << "Wrong join type " << join->JoinKind << Endl;
        return false;
    }

    auto leftIUs = join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = join->GetRightInput()->GetOutputIUs();

    TVector<TExpression> pushLeft;
    TVector<TExpression> pushRight;
    TVector<TExpression> remainingFilters;

    bool canPushRight = join->JoinKind == "Inner";

    for (const auto& filter : join->JoinFilters) {
        if (IUSetDiff(filter.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true), leftIUs).empty()) {
            pushLeft.push_back(filter);
        } else if (IUSetDiff(filter.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true), rightIUs).empty() && canPushRight) {
            pushRight.push_back(filter);
        } else {
            remainingFilters.push_back(filter);
        }
    }

    if (!pushLeft.size() && !pushRight.size()) {
        return false;
    }

    auto leftInput = join->GetLeftInput();
    auto rightInput = join->GetRightInput();

    // When join conditions have been set for the join, replicate constant conditions from left/right side
    // to the other side. This optimization is enabled for inner joins
    // FIXME: Check if we can expand this to Left Joins
   
    if (join->JoinKind == "Inner") {
        TVector<TExpression> pushConstantCondsRight;
        TVector<TExpression> pushConstantCondsLeft;

        // Check if left constant condition on the left key can be pushed to right side
        for (const auto& expr : pushLeft) {
            if (expr.MaybeConstantCondition()) {
                auto iu = expr.GetInputIUs()[0];
                if (auto it = std::find_if(join->JoinKeys.begin(), join->JoinKeys.end(), [&iu](const std::pair<TInfoUnit, TInfoUnit>& cond)
                    {return iu == cond.first;}); it != join->JoinKeys.end()) {
                    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> mapping;
                    mapping.insert({iu, it->second});
                    auto rightExpr = expr.ApplyRenames(mapping);
                    pushConstantCondsRight.push_back(rightExpr);
                }
            }
        }
        // Check if right constant condition on the right key can be pushed to left side
        for (const auto& expr : pushRight) {
            if (expr.MaybeConstantCondition()) {
                auto iu = expr.GetInputIUs()[0];
                if (auto it = std::find_if(join->JoinKeys.begin(), join->JoinKeys.end(), [&iu](const std::pair<TInfoUnit, TInfoUnit>& cond)
                    {return iu == cond.second;}); it != join->JoinKeys.end()) {
                    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> mapping;
                    mapping.insert({iu, it->first});
                    auto leftExpr = expr.ApplyRenames(mapping);
                    pushConstantCondsLeft.push_back(leftExpr);
                }
            }
        }

        pushRight.insert(pushRight.end(), pushConstantCondsRight.begin(), pushConstantCondsRight.end());
        pushLeft.insert(pushLeft.end(), pushConstantCondsLeft.begin(), pushConstantCondsLeft.end());
    }

    if (pushLeft.size()) {
        auto leftExpr = MakeConjunction(pushLeft, props.PgSyntax);
        leftInput = MakeIntrusive<TOpFilter>(leftInput, input->Pos, leftExpr);
    }

    if (pushRight.size()) {
        auto rightExpr = MakeConjunction(pushRight, props.PgSyntax);
        rightInput = MakeIntrusive<TOpFilter>(rightInput, input->Pos, rightExpr);
    }

    join->SetLeftInput(leftInput);
    join->SetRightInput(rightInput);
    join->JoinFilters = remainingFilters;

    return true;
}
}
}
