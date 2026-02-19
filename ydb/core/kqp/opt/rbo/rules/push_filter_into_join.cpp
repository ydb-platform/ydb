#include "kqp_rules_include.h"

namespace {
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

const THashSet<TString> CmpOperators{"=", "<", ">", "<=", ">="};

TExprNode::TPtr PruneCast(TExprNode::TPtr node) {
    if (node->IsCallable("ToPg") || node->IsCallable("PgCast")) {
        return node->Child(0);
    }
    return node;
}

bool IsNullRejectingPredicate(const TExpression &filter) {
#ifdef DEBUG_PREDICATE
    YQL_CLOG(TRACE, CoreDq) << "IsNullRejectingPredicate: " << filter.ToString();
#endif
    auto predicate = filter.GetExpressionBody();
    if (predicate->IsCallable("PgResolvedOp") && CmpOperators.contains(TString(predicate->Child(0)->Content()))) {
        auto left = PruneCast(predicate->Child(2));
        auto right = PruneCast(predicate->Child(3));
        return (left->IsCallable("PgConst") || right->IsCallable("PgConst"));
    }
    return false;
}
}

namespace NKikimr {
namespace NKqp {

// FIXME: We currently support pushing filter into Inner, Cross and Left Join
TIntrusivePtr<IOperator> TPushFilterIntoJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    // Only handle Inner and Cross join at this time
    auto join = CastOperator<TOpJoin>(filter->GetInput());

    // Make sure the join and its inputs are single consumer
    if (!join->IsSingleConsumer()) {
        YQL_CLOG(TRACE, CoreDq) << "Multiple consumers in push filter rule";
        return input;
    }

    if (join->JoinKind != "Inner" && join->JoinKind != "Cross" && join->JoinKind != "Left") {
        YQL_CLOG(TRACE, CoreDq) << "Wrong join type " << join->JoinKind << Endl;
        return input;
    }

    auto output = input;
    auto leftIUs = join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = join->GetRightInput()->GetOutputIUs();

    // Break the filter into join conditions and other conjuncts
    // Join conditions can be pushed into the join operator and conjucts can either be pushed
    // or left on top of the join
    auto conjuncts = filter->FilterExpr.SplitConjunct();

    // Check if we need a top level filter
    TVector<TExpression> topLevelPreds;
    TVector<TExpression> pushLeft;
    TVector<TExpression> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (const auto& conj : conjuncts) {
        if (conj.MaybeJoinCondition()) {
            TJoinCondition cond(conj);

            if (IUSetDiff({cond.GetLeftIU()}, leftIUs).empty() && IUSetDiff({cond.GetRightIU()}, rightIUs).empty()) {
                joinConditions.push_back(std::make_pair(cond.GetLeftIU(), cond.GetRightIU()));
                continue;
            } else if (IUSetDiff({cond.GetLeftIU()}, rightIUs).empty() && IUSetDiff({cond.GetRightIU()}, leftIUs).empty()) {
                joinConditions.push_back(std::make_pair(cond.GetRightIU(), cond.GetLeftIU()));
                continue;
            }
        }

        if (IUSetDiff(conj.GetInputIUs(true, true), leftIUs).empty()) {
            pushLeft.push_back(conj);
        } else if (IUSetDiff(conj.GetInputIUs(true, true), rightIUs).empty()) {
            pushRight.push_back(conj);
        } else {
            topLevelPreds.push_back(conj);
        }
    
    }

    if (!pushLeft.size() && !pushRight.size() && !joinConditions.size()) {
        YQL_CLOG(TRACE, CoreDq) << "Nothing to push";
        return input;
    }

    if (!joinConditions.empty()) {
        join->JoinKind = "Inner";
    }

    join->JoinKeys.insert(join->JoinKeys.end(), joinConditions.begin(), joinConditions.end());
    auto leftInput = join->GetLeftInput();
    auto rightInput = join->GetRightInput();

    if (pushLeft.size()) {
        auto leftExpr = MakeConjunction(pushLeft, props.PgSyntax);
        leftInput = MakeIntrusive<TOpFilter>(leftInput, input->Pos, leftExpr);
    }

    if (pushRight.size()) {
        if (join->JoinKind == "Left") {
            TVector<TExpression> predicatesForRightSide;
            for (const auto &predicate : pushRight) {
                if (IsNullRejectingPredicate(predicate)) {
                    predicatesForRightSide.push_back(predicate);
                } else {
                    topLevelPreds.push_back(predicate);
                }
            }
            if (predicatesForRightSide.size()) {
                auto rightExpr = MakeConjunction(pushRight, props.PgSyntax);
                rightInput = MakeIntrusive<TOpFilter>(rightInput, input->Pos, rightExpr);
                join->JoinKind = "Inner";
            } else if (!pushLeft.size()) {
                return input;
            }
        } else {
            auto rightExpr = MakeConjunction(pushRight, props.PgSyntax);
            rightInput = MakeIntrusive<TOpFilter>(rightInput, input->Pos, rightExpr);
        }
    }

    join->SetLeftInput(leftInput);
    join->SetRightInput(rightInput);

    if (topLevelPreds.size()) {
        auto topFilterExpr = MakeConjunction(topLevelPreds, props.PgSyntax);
        output =  MakeIntrusive<TOpFilter>(join, input->Pos, topFilterExpr);
    } else {
        output = join;
    }

    return output;
}
}
}