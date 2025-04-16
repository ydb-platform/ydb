#include "kqp_new_rbo_rules.h"

#include <yql/essentials/utils/log/log.h>



using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp; 

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
    else if(input->IsList()){
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        return ctx.Builder(input->Pos()).List().Add(std::move(newChildren)).Seal().Build();
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
}

namespace NKikimr {
namespace NKqp {

std::shared_ptr<IOperator> TPushFilterRule::SimpleTestAndApply(const std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    // Only handle Inner and Cross join at this time
    auto join = CastOperator<TOpJoin>(filter->GetInput());
    if (join->JoinKind != "Inner" && join->JoinKind != "Cross") {
        YQL_CLOG(TRACE, CoreDq) << "Wrong join type";
        return input;
    }

    auto leftIUs = join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = join->GetRightInput()->GetOutputIUs();

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

    if (!pushLeft.size() && !pushRight.size() && !joinConditions.size()) {
        YQL_CLOG(TRACE, CoreDq) << "Nothing to push";
        return input;
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

    TString joinKind = joinNode.JoinKind().StringValue();
    if (joinKind == "Cross" && joinConditions.size()) {
        joinKind = "Inner";
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
            .JoinKind().Value(joinKind).Build()
            .JoinKeys()
                .Add(joinConds)
            .Build()
            .Done().Ptr();

    if (topLevelPreds.size()) {
        auto newFilter = BuildFilterFromConjuncts(newJoin, topLevelPreds, ctx);
        return std::make_shared<TOpFilter>(newFilter);
    } else {
        return std::make_shared<TOpJoin>(newJoin);
    }
}

bool TAssignStagesRule::TestAndApply(std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    auto nodeName = input->Node->Content();

    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return false;
    }

    for (auto & c : input->Children) {
        if (!c->Props.StageId.has_value()) {
            YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " child with unassigned stage";
            return false;
        }
    }

    if (input->Kind == EOperator::EmptySource || input->Kind == EOperator::Source) {
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        TString readName;
        if (input->Kind == EOperator::Source) {
            readName = CastOperator<TOpRead>(input)->TableName;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;

    }

    else if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftStage = join->GetLeftInput()->Props.StageId;
        auto rightStage = join->GetRightInput()->Props.StageId;

        auto newStageId = props.StageGraph.AddStage();
        join->Props.StageId = newStageId;

        // For cross-join we build a stage with map and broadcast connections
        if (join->JoinKind == "Cross") {
            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TMapConnection>());
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TBroadcastConnection>());
        }
        
        // For inner join (we don't support other joins yet) we build a new stage
        // with GraceJoinCore and connect inputs via Shuffle connections
        else {
            TVector<TInfoUnit> leftShuffleKeys;
            TVector<TInfoUnit> rightShuffleKeys;
            for (auto k : join->JoinKeys) {
                leftShuffleKeys.push_back(k.first);
                rightShuffleKeys.push_back(k.second);
            }

            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TShuffleConnection>(leftShuffleKeys));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TShuffleConnection>(rightShuffleKeys));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    }
    else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        input->Props.StageId = input->Children[0]->Props.StageId;
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    }
    else if (input->Kind == EOperator::Limit) {
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(input->Children[0]->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TUnionAllConnection>());
    }
    else {
        Y_ENSURE(true, "Unknown operator encountered");
    }

    return true;
}

TRuleBasedStage RuleStage1 = TRuleBasedStage({std::make_shared<TPushFilterRule>()}, true);
TRuleBasedStage RuleStage2 = TRuleBasedStage({std::make_shared<TAssignStagesRule>()}, false);

}
}