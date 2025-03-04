#include "kqp_new_rbo_rules.h"

#include <yql/essentials/utils/log/log.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql::NNodes;


TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext& ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(arg)
            .Name(member.Name())
            .Done().Ptr();
    }
    else if (input->IsCallable()){
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        return ctx.Builder(input->Pos()).Callable(input->Content()).Add(std::move(newChildren)).Seal().Build();
    }
    else {
        return input;
    }
}

TExprNode::TPtr BuildFilterFromConjuncts(TExprNode::TPtr input, TVector<TExprNode::TPtr> conjuncts, TExprContext& ctx) {
    auto arg = Build<TCoArgument>(ctx, input->Pos()).Name("lambda_arg").Done();
    TExprNode::TPtr lambda;

    if (conjuncts.size()==1) {
        auto body = TExprBase(ReplaceArg(conjuncts[0], arg.Ptr(), ctx));

        lambda = Build<TCoLambda>(ctx, input->Pos())
            .Args(arg)
            .Body(body)
            .Done().Ptr();
    }
    else {
        TVector<TExprNode::TPtr> newConjuncts;

        for (auto c : conjuncts ) {
            newConjuncts.push_back( ReplaceArg(c, arg.Ptr(), ctx));
        }

        lambda = Build<TCoLambda>(ctx, input->Pos())
            .Args(arg)
            .Body<TCoAnd>()
                .Add(newConjuncts)
            .Build()
            .Done().Ptr();
    }

    return Build<TKqpOpFilter>(ctx, input->Pos())
        .Input(input)
        .Lambda(lambda)
        .Done().Ptr();
}

bool TPushFilterRule::TestAndApply(std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config) {

    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = std::static_pointer_cast<TOpFilter>(input);
    if (filter->Children[0]->Kind != EOperator::Join) {
        return false;
    }

    auto join = std::static_pointer_cast<TOpJoin>(filter->Children[0]);
    auto leftIUs = join->Children[0]->GetOutputIUs();
    auto rightIUs = join->Children[1]->GetOutputIUs();

    // Break the filter into join conditions and other conjuncts
    // Join conditions can be pushed into the join operator and conjucts can either be pushed
    // or left on top of the join
    
    auto conjunctInfo = filter->GetConjuctInfo();

    // Check if we need a top level filter
    TVector<TExprNode::TPtr> topLevelPreds;
    TVector<TExprNode::TPtr> pushLeft;
    TVector<TExprNode::TPtr> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (auto c : conjunctInfo.Filters) {
        if (!IUSetDiff(c.second, leftIUs).size()) {
            pushLeft.push_back(c.first);
        }
        else if (!IUSetDiff(c.second, rightIUs).size()) {
            pushRight.push_back(c.first);
        }
        else {
            topLevelPreds.push_back(c.first);
        }
    }

    for (auto c: conjunctInfo.JoinConditions) {
        if (!IUSetDiff({std::get<1>(c)}, leftIUs).size() && !IUSetDiff({std::get<2>(c)}, rightIUs).size()) {
            joinConditions.push_back(std::make_pair(std::get<1>(c), std::get<2>(c)));
        }
        else if (!IUSetDiff({std::get<1>(c)}, rightIUs).size() && !IUSetDiff({std::get<2>(c)}, leftIUs).size()) {
            joinConditions.push_back(std::make_pair(std::get<2>(c), std::get<1>(c)));
        }
        else {
            TVector<TInfoUnit> vars = {std::get<1>(c)};
            vars.push_back(std::get<2>(c));
            if (!IUSetDiff(vars, leftIUs).size()) {
                pushLeft.push_back(std::get<0>(c));
            }
            else {
                pushRight.push_back(std::get<0>(c));
            }
        }
    }

    auto joinNode = TKqpOpJoin(join->Node);
    auto filterNode = TKqpOpFilter(filter->Node);
    auto leftArg = joinNode.LeftInput().Ptr();
    auto rightArg = joinNode.RightInput().Ptr();

    if (pushLeft.size()) {
        leftArg = BuildFilterFromConjuncts(leftArg, pushLeft, ctx);
    }
    if (pushRight.size()) {
        rightArg = BuildFilterFromConjuncts(rightArg, pushRight, ctx);
    }

    TVector<TExprNode::TPtr> joinConds;
    for (auto cond : joinNode.JoinKeys()) {
        joinConds.push_back(cond.Ptr());
    }
    for (auto cond : joinConditions) {
        joinConds.push_back(Build<TDqJoinKeyTuple>(ctx, joinNode.Pos())
            .LeftLabel().Value(cond.first.Alias).Build()
            .LeftColumn().Value(cond.first.ColumnName).Build()
            .RightLabel().Value(cond.second.Alias).Build()
            .RightColumn().Value(cond.second.ColumnName).Build()
            .Done().Ptr());
    }

    auto newJoin = Build<TKqpOpJoin>(ctx, filterNode.Pos())
            .LeftInput(leftArg)
            .RightInput(rightArg)
            .LeftLabel(joinNode.LeftLabel())
            .RightLabel(joinNode.RightLabel())
            .JoinKind(joinNode.JoinKind())
            .JoinKeys()
                .Add(joinConds)
            .Build()
            .Done().Ptr();

    if (topLevelPreds.size()) {
        auto newFilter = BuildFilterFromConjuncts(newJoin, topLevelPreds, ctx);
        input = std::make_shared<TOpFilter>(newFilter);
        return true;
    } else {
        input = std::make_shared<TOpJoin>(newJoin);
        return true;
    }
}

TVector<std::shared_ptr<IRule>> RuleStage1 = {
    std::make_shared<TPushFilterRule>()
};

}
}