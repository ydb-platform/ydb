#include "kqp_rules_include.h"

namespace {
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsSimpleConstant(const TExprBase& input) {
    if (auto maybeData = input.Maybe<TCoDataCtor>()) {
        auto data = maybeData.Cast();
        return data.Maybe<TCoBool>() || data.Maybe<TCoFloat>() || data.Maybe<TCoDouble>() || data.Maybe<TCoInt8>() || data.Maybe<TCoInt16>() ||
               data.Maybe<TCoInt32>() || data.Maybe<TCoInt64>() || data.Maybe<TCoUint8>() || data.Maybe<TCoUint16>() || data.Maybe<TCoUint32>() ||
               data.Maybe<TCoUint64>() || data.Maybe<TCoUtf8>() || data.Maybe<TCoString>() || data.Maybe<TCoDate>() || data.Maybe<TCoDate32>() ||
               data.Maybe<TCoDatetime>() || data.Maybe<TCoDatetime64>() || data.Maybe<TCoTimestamp64>() || data.Maybe<TCoInterval64>() ||
               data.Maybe<TCoInterval>() || data.Maybe<TCoTimestamp>();
    }
    return false;
}

bool IsAllowedComparator(TExprNode::TPtr input) {
    return input->IsCallable({"==", "<=", ">=", "<", ">", "!=", "StringContains", "StartsWith", "EndsWith", "StringContainsIgnoreCase", "StartsWithIgnoreCase",
                              "EndsWithIgnoreCase"});
}

bool IsSimpleColumnAccess(const TExprBase& colAccess, const TExprBase& columnArg) {
    if (auto maybeMember = colAccess.Maybe<TCoMember>()) {
        return maybeMember.Cast().Struct().Ptr().Get() == columnArg.Ptr().Get();
    }
    return false;
}

template <typename T>
bool IsNullRejecting(const TExprBase& input, const TExprBase& lambdaArg) {
    auto compare = input.Cast<T>();
    auto leftArg = compare.Left();
    auto rightArg = compare.Right();
    return (IsSimpleColumnAccess(leftArg, lambdaArg) && IsSimpleConstant(rightArg)) || (IsSimpleColumnAccess(rightArg, lambdaArg) && IsSimpleConstant(leftArg));
}

TExprBase PruneNot(const TExprBase& node) {
    if (auto maybeNot = node.Maybe<TCoNot>()) {
        return maybeNot.Cast().Value();
    }
    return node;
}

bool IsNullRejectingPredicate(const TExpression& filter) {
    auto lambda = TCoLambda(filter.GetLambda());
    auto arg = lambda.Args().Arg(0);

    auto predicate = PruneNot(lambda.Body());
    if (IsAllowedComparator(predicate.Ptr())) {
        return IsNullRejecting<TCoCompare>(predicate, arg);
    }
    return false;
}
}

namespace NKikimr {
namespace NKqp {

// FIXME: We currently support pushing filter into Inner, Cross and Left Join
TIntrusivePtr<IOperator> TPushFilterIntoJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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

    if (join->JoinKind != "Inner" && join->JoinKind != "Cross" && join->JoinKind != "Left" && join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly") {
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

    bool canPushRight = join->JoinKind != "LeftSemi" && join->JoinKind != "LeftOnly";

    for (const auto& conj : conjuncts) {
        if (conj.MaybeEquiJoinCondition()) {
            TEquiJoinCondition cond(conj);

            // We cannot push filter into join conditions of a LeftOnly join - will break semantics
            if(join->JoinKind != "LeftOnly") {
                if (IUSetDiff({cond.GetLeftIU()}, leftIUs).empty() && IUSetDiff({cond.GetRightIU()}, rightIUs).empty()) {
                    joinConditions.push_back(std::make_pair(cond.GetLeftIU(), cond.GetRightIU()));
                    continue;
                } else if (IUSetDiff({cond.GetLeftIU()}, rightIUs).empty() && IUSetDiff({cond.GetRightIU()}, leftIUs).empty()) {
                    joinConditions.push_back(std::make_pair(cond.GetRightIU(), cond.GetLeftIU()));
                    continue;
                }
            }
        }

        if (IUSetDiff(conj.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true), leftIUs).empty()) {
            pushLeft.push_back(conj);
        } else if (IUSetDiff(conj.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true), rightIUs).empty() && canPushRight) {
            pushRight.push_back(conj);
        } else {
            topLevelPreds.push_back(conj);
        }
    
    }

    if (!pushLeft.size() && !pushRight.size() && !joinConditions.size()) {
        YQL_CLOG(TRACE, CoreDq) << "Nothing to push";
        return input;
    }

    if ((join->JoinKind == "Cross" || join->JoinKind == "Left" ) && !joinConditions.empty()) {
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
            if (!predicatesForRightSide.empty()) {
                auto rightExpr = MakeConjunction(predicatesForRightSide, props.PgSyntax);
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