#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
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

TExprNode::TPtr BuildFilterLambdaFromConjuncts(TPositionHandle pos, TVector<TFilterInfo> conjuncts, TExprContext& ctx) {
    auto arg = Build<TCoArgument>(ctx, pos).Name("lambda_arg").Done();
    TExprNode::TPtr lambda;

    if (conjuncts.size()==1) {
        auto body = TExprBase(ReplaceArg(conjuncts[0].FilterBody, arg.Ptr(), ctx));

        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body(body)
            .Done().Ptr();
    }
    else {
        TVector<TExprNode::TPtr> newConjuncts;

        for (auto c : conjuncts ) {
            newConjuncts.push_back( ReplaceArg(c.FilterBody, arg.Ptr(), ctx));
        }

        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body<TCoAnd>()
                .Add(newConjuncts)
            .Build()
            .Done().Ptr();
    }
}

THashSet<TString> CmpOperators{"=", "<", ">", "<=", ">="};

TExprNode::TPtr PruneCast(TExprNode::TPtr node) {
    if (node->IsCallable("ToPg") || node->IsCallable("PgCast")) {
        return node->Child(0);
    }
    return node;
}

bool IsNullRejectingPredicate(const TFilterInfo& filter, TExprContext& ctx) {
    Y_UNUSED(ctx);
#ifdef DEBUG_PREDICATE
    YQL_CLOG(TRACE, CoreDq) << "IsNullRejectingPredicate: " << NYql::KqpExprToPrettyString(TExprBase(filter.FilterBody), ctx);
#endif
    auto predicate = filter.FilterBody;
    if (predicate->IsCallable("PgResolvedOp") && CmpOperators.contains(TString(predicate->Child(0)->Content()))) {
        auto left = PruneCast(predicate->Child(2));
        auto right = PruneCast(predicate->Child(3));
        return (left->IsCallable("PgConst") || right->IsCallable("PgConst"));
    }
    return false;
}
}  // namespace

namespace NKikimr {
namespace NKqp {

std::shared_ptr<IOperator> TPushFilterRule::SimpleTestAndApply(const std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    Y_UNUSED(kqpCtx);
    Y_UNUSED(typeCtx);
    Y_UNUSED(config);
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
    if (join->JoinKind != "Inner" && join->JoinKind != "Cross" && to_lower(join->JoinKind) != "left") {
        YQL_CLOG(TRACE, CoreDq) << "Wrong join type " << join->JoinKind << Endl;
        return input;
    }

    auto leftIUs = join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = join->GetRightInput()->GetOutputIUs();

    // Break the filter into join conditions and other conjuncts
    // Join conditions can be pushed into the join operator and conjucts can either be pushed
    // or left on top of the join
    
    auto conjunctInfo = filter->GetConjuctInfo();

    // Check if we need a top level filter
    TVector<TFilterInfo> topLevelPreds;
    TVector<TFilterInfo> pushLeft;
    TVector<TFilterInfo> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (auto f : conjunctInfo.Filters) {
        if (!IUSetDiff(f.FilterIUs, leftIUs).size()) {
            pushLeft.push_back(f);
        }
        else if (!IUSetDiff(f.FilterIUs, rightIUs).size()) {
            pushRight.push_back(f);
        }
        else {
            topLevelPreds.push_back(f);
        }
    }

    for (auto c: conjunctInfo.JoinConditions) {
        if (!IUSetDiff({c.LeftIU}, leftIUs).size() && !IUSetDiff({c.RightIU}, rightIUs).size()) {
            joinConditions.push_back(std::make_pair(c.LeftIU, c.RightIU));
        }
        else if (!IUSetDiff({c.LeftIU}, rightIUs).size() && !IUSetDiff({c.RightIU}, leftIUs).size()) {
            joinConditions.push_back(std::make_pair(c.RightIU, c.LeftIU));
        }
        else {
            TVector<TInfoUnit> vars = {c.LeftIU};
            vars.push_back(c.RightIU);
            if (!IUSetDiff(vars, leftIUs).size()) {
                pushLeft.push_back(TFilterInfo(c.ConjunctExpr, vars));
            }
            else {
                pushRight.push_back(TFilterInfo(c.ConjunctExpr, vars));
            }
        }
    }

    if (!pushLeft.size() && !pushRight.size() && !joinConditions.size()) {
        YQL_CLOG(TRACE, CoreDq) << "Nothing to push";
        return input;
    }

    join->JoinKeys.insert(join->JoinKeys.end(), joinConditions.begin(), joinConditions.end());
    auto leftInput = join->GetLeftInput();
    auto rightInput = join->GetRightInput();

    if (pushLeft.size()) {
        auto leftLambda = BuildFilterLambdaFromConjuncts(leftInput->Node->Pos(), pushLeft, ctx);
        leftInput = std::make_shared<TOpFilter>(leftInput, leftLambda, ctx, leftInput->Node->Pos());
    }

    if (pushRight.size()) {
        if (join->JoinKind == "Left") {
            TVector<TFilterInfo> predicatesForRightSide;
            for (const auto& predicate : pushRight) {
                if (IsNullRejectingPredicate(predicate, ctx)) {
                    predicatesForRightSide.push_back(predicate);
                } else {
                    topLevelPreds.push_back(predicate);
                }
            }
            if (predicatesForRightSide.size()) {
                auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Node->Pos(), pushRight, ctx);
                rightInput = std::make_shared<TOpFilter>(rightInput, rightLambda, ctx, rightInput->Node->Pos());
                join->JoinKind = "Inner";
            } else {
                return input;
            }
        } else {
            auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Node->Pos(), pushRight, ctx);
            rightInput = std::make_shared<TOpFilter>(rightInput, rightLambda, ctx, rightInput->Node->Pos());
        }
    }

    if (join->JoinKind == "Cross" && joinConditions.size()) {
        join->JoinKind = "Inner";
    }

    join->Children[0] = leftInput;
    join->Children[1] = rightInput;

    if (topLevelPreds.size()) {
        auto topFilterLambda = BuildFilterLambdaFromConjuncts(join->Node->Pos(), topLevelPreds, ctx);
        return std::make_shared<TOpFilter>(join, topFilterLambda, ctx, join->Node->Pos());
    } else {
        return join;
    }
}

bool TAssignStagesRule::TestAndApply(std::shared_ptr<IOperator> & input, TExprContext& ctx, 
    const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
    TTypeAnnotationContext& typeCtx, 
    const TKikimrConfiguration::TPtr& config,
    TPlanProps& props) {

    Y_UNUSED(ctx);
    Y_UNUSED(kqpCtx);
    Y_UNUSED(typeCtx);
    Y_UNUSED(config);
    Y_UNUSED(props);

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

        TString readName;
        if (input->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(input);
            auto newStageId = props.StageGraph.AddSourceStage(opRead->GetOutputIUs());
            input->Props.StageId = newStageId;
            readName = opRead->TableName;
        }
        else {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;

    }

    else if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftStage = join->GetLeftInput()->Props.StageId;
        auto rightStage = join->GetRightInput()->Props.StageId;

        auto newStageId = props.StageGraph.AddStage();
        join->Props.StageId = newStageId;

        bool isLeftSourceStage = props.StageGraph.IsSourceStage(*leftStage);
        bool isRightSourceStage = props.StageGraph.IsSourceStage(*rightStage);

        // For cross-join we build a stage with map and broadcast connections
        if (join->JoinKind == "Cross") {
            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TMapConnection>(isLeftSourceStage));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TBroadcastConnection>(isRightSourceStage));
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

            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TShuffleConnection>(leftShuffleKeys, isLeftSourceStage));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TShuffleConnection>(rightShuffleKeys, isRightSourceStage));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    }
    else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = input->Children[0];
        auto prevStageId = *(childOp->Props.StageId);

        // If the child operator is a source, it requires its own stage
        // So we have build a new stage for current operator
        if (childOp->Kind == EOperator::Source) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TSourceConnection>());
        }
        else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    }
    else if (input->Kind == EOperator::Limit) {
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(input->Children[0]->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TUnionAllConnection>(props.StageGraph.IsSourceStage(prevStageId)));
    }
    else {
        Y_ENSURE(true, "Unknown operator encountered");
    }

    return true;
}

TRuleBasedStage RuleStage1 = TRuleBasedStage({std::make_shared<TPushFilterRule>()}, true);
TRuleBasedStage RuleStage2 = TRuleBasedStage({std::make_shared<TAssignStagesRule>()}, false);

}
}  // namespace NKikimr