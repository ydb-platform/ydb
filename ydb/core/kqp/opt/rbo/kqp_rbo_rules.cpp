#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <typeinfo>

using namespace NYql::NNodes;

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

TExprNode::TPtr ReplaceArg(TExprNode::TPtr input, TExprNode::TPtr arg, TExprContext &ctx) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        // clang-format off
        return Build<TCoMember>(ctx, input->Pos())
            .Struct(arg)
            .Name(member.Name())
        .Done().Ptr();
        // clang-format on
    } else if (input->IsCallable()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .Callable(input->Content())
            .Add(std::move(newChildren))
            .Seal()
        .Build();
        // clang-format on
    } else if (input->IsList()) {
        TVector<TExprNode::TPtr> newChildren;
        for (auto c : input->Children()) {
            newChildren.push_back(ReplaceArg(c, arg, ctx));
        }
        // clang-format off
        return ctx.Builder(input->Pos())
            .List()
            .Add(std::move(newChildren))
            .Seal()
        .Build();
        // clang-format on
    } else {
        return input;
    }
}

TExprNode::TPtr FindMemberArg(TExprNode::TPtr input) {
    if (input->IsCallable("Member")) {
        auto member = TCoMember(input);
        return member.Struct().Ptr();
    } else if (input->IsCallable()) {
        for (auto c : input->Children()) {
            if (auto arg = FindMemberArg(c))
                return arg;
        }
    } else if (input->IsList()) {
        for (auto c : input->Children()) {
            if (auto arg = FindMemberArg(c)) {
                return arg;
            }
        }
    }
    return TExprNode::TPtr();
}

TExprNode::TPtr BuildFilterLambdaFromConjuncts(TPositionHandle pos, TVector<TFilterInfo> conjuncts, TExprContext &ctx) {
    auto arg = Build<TCoArgument>(ctx, pos).Name("lambda_arg").Done();
    TExprNode::TPtr lambda;

    if (conjuncts.size() == 1) {
        auto filterInfo = conjuncts[0];
        auto body = ReplaceArg(filterInfo.FilterBody, arg.Ptr(), ctx);
        if (!filterInfo.FromPg) {
            body = ctx.Builder(body->Pos()).Callable("FromPg").Add(0, body).Seal().Build();
        }

        // clang-format off
        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body(body)
        .Done().Ptr();
        // clang-format on
    } else {
        TVector<TExprNode::TPtr> newConjuncts;

        for (auto c : conjuncts) {
            auto body = ReplaceArg(c.FilterBody, arg.Ptr(), ctx);
            if (!c.FromPg) {
                body = ctx.Builder(body->Pos()).Callable("FromPg").Add(0, body).Seal().Build();
            }
            newConjuncts.push_back(ReplaceArg(body, arg.Ptr(), ctx));
        }

        // clang-format off
        return Build<TCoLambda>(ctx, pos)
            .Args(arg)
            .Body<TCoAnd>()
                .Add(newConjuncts)
            .Build()
        .Done().Ptr();
        // clang-format on
    }
}

THashSet<TString> CmpOperators{"=", "<", ">", "<=", ">="};

TExprNode::TPtr PruneCast(TExprNode::TPtr node) {
    if (node->IsCallable("ToPg") || node->IsCallable("PgCast")) {
        return node->Child(0);
    }
    return node;
}

TVector<TInfoUnit> GetHashableKeys(const std::shared_ptr<IOperator> &input) {
    if (!input->Type) {
        return input->GetOutputIUs();
    }

    const auto *inputType = input->Type;
    TVector<TInfoUnit> hashableKeys;
    const auto* structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto &item : structType->GetItems()) {
        if (item->GetItemType()->IsHashable()) {
            hashableKeys.push_back(TInfoUnit(TString(item->GetName())));
        }
    }

    return hashableKeys;
}

bool IsNullRejectingPredicate(const TFilterInfo &filter, TExprContext &ctx) {
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
} // namespace

namespace NKikimr {
namespace NKqp {

// Currently we only extract simple expressions where there is only one variable on either side

bool TExtractJoinExpressionsRule::TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto conjInfo = filter->GetConjunctInfo(props);

    TVector<std::pair<int, TExprNode::TPtr>> matchedConjuncts;

    YQL_CLOG(TRACE, CoreDq) << "Testing expr extraction";

    for (size_t idx = 0; idx < conjInfo.Filters.size(); idx++) {
        auto f = conjInfo.Filters[idx];
        YQL_CLOG(TRACE, CoreDq) << "IUs in filter " << f.FilterIUs.size();

        if (f.FilterIUs.size() == 2) {
            auto predicate = f.FilterBody;

            if (predicate->IsCallable("FromPg")) {
                predicate = predicate->Child(0);
            }

            if (predicate->IsCallable("PgResolvedOp") && predicate->Child(0)->Content() == "=") {
                auto leftSide = predicate->Child(2);
                auto rightSide = predicate->Child(3);

                if (leftSide->IsCallable("Member") && rightSide->IsCallable("Member")) {
                    continue;
                }

                TVector<TInfoUnit> leftIUs;
                TVector<TInfoUnit> rightIUs;
                GetAllMembers(leftSide, leftIUs, props);
                GetAllMembers(rightSide, rightIUs, props);

                if (leftIUs.size() == 1 && rightIUs.size() == 1) {
                    matchedConjuncts.push_back(std::make_pair(idx, f.FilterBody));
                }
            }
        }
    }

    if (matchedConjuncts.size()) {
        TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements;
        TNodeOnNodeOwnedMap replaceMap;

        for (auto [idx, conj] : matchedConjuncts) {
            if (conj->IsCallable("FromPg")) {
                conj = conj->Child(0);
            }

            auto leftSide = conj->Child(2);
            auto rightSide = conj->Child(3);

            auto memberArg = FindMemberArg(leftSide);

            if (!leftSide->IsCallable("Member")) {
                TString newName = "_rbo_arg_" + std::to_string(props.InternalVarIdx++);
                auto lambda_arg = Build<TCoArgument>(ctx.ExprCtx, leftSide->Pos()).Name("arg").Done().Ptr();

                // clang-format off
                auto mapLambda = Build<TCoLambda>(ctx.ExprCtx, leftSide->Pos())
                    .Args({lambda_arg})
                    .Body(ReplaceArg(leftSide, lambda_arg, ctx.ExprCtx))
                    .Done().Ptr();
                // clang-format on

                mapElements.push_back(std::make_pair(TInfoUnit(newName), mapLambda));

                // clang-format off
                auto newLeftSide = Build<TCoMember>(ctx.ExprCtx, leftSide->Pos())
                    .Struct(memberArg)
                    .Name().Value(newName).Build()
                    .Done().Ptr();
                // clang-format on

                replaceMap[leftSide] = newLeftSide;
            }

            if (!rightSide->IsCallable("Member")) {
                TString newName = "_rbo_arg_" + std::to_string(props.InternalVarIdx++);
                auto lambda_arg = Build<TCoArgument>(ctx.ExprCtx, rightSide->Pos()).Name("arg").Done().Ptr();

                // clang-format off
                auto mapLambda = Build<TCoLambda>(ctx.ExprCtx, rightSide->Pos())
                    .Args({lambda_arg})
                    .Body(ReplaceArg(rightSide, lambda_arg, ctx.ExprCtx))
                    .Done().Ptr();
                // clang-format on

                mapElements.push_back(std::make_pair(TInfoUnit(newName), mapLambda));

                // clang-format off
                auto newRightSide = Build<TCoMember>(ctx.ExprCtx, leftSide->Pos())
                    .Struct(memberArg)
                    .Name().Value(newName).Build()
                    .Done().Ptr();
                // clang-format on

                replaceMap[rightSide] = newRightSide;
            }
        }

        TOptimizeExprSettings settings(&ctx.TypeCtx);
        TExprNode::TPtr newFilterLambda;
        RemapExpr(filter->FilterLambda, newFilterLambda, replaceMap, ctx.ExprCtx, settings);
        filter->FilterLambda = newFilterLambda;

        auto newMap = std::make_shared<TOpMap>(filter->GetInput(), input->Pos, mapElements, false);
        filter->Children[0] = newMap;
        return true;
    }

    return false;
}

// Rewrite a single scalar subplan into a cross-join

bool TInlineScalarSubplanRule::TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    auto scalarIUs = input->GetScalarSubplanIUs(props);
    if (scalarIUs.empty()) {
        return false;
    }

    auto scalarIU = scalarIUs[0];
    auto subplan = props.ScalarSubplans.PlanMap.at(scalarIU);
    auto subplanResIU = subplan->GetOutputIUs()[0];
    auto subplanResType = subplan->GetIUType(subplanResIU);

    auto unaryOp = std::dynamic_pointer_cast<IUnaryOperator>(input);
    Y_ENSURE(unaryOp);

    auto child = unaryOp->GetInput();

    auto emptySource = std::make_shared<TOpEmptySource>(subplan->Pos);

    // FIXME: This works only for postgres types, because they are null-compatible
    // For YQL types we will need to handle optionality
    auto nullExpr = ctx.ExprCtx.NewCallable(subplan->Pos, "Nothing", {ExpandType(subplan->Pos, *subplanResType, ctx.ExprCtx)});

    // clang-format off
    auto mapLambda = Build<TCoLambda>(ctx.ExprCtx, subplan->Pos)
                    .Args({"arg"})
                    .Body(nullExpr)
                    .Done().Ptr();
    // clang-format on

    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements = {std::make_pair(scalarIU, mapLambda)};
    auto map = std::make_shared<TOpMap>(emptySource, subplan->Pos, mapElements, true);

    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> renameElements = {std::make_pair(scalarIU, subplanResIU)};
    auto rename = std::make_shared<TOpMap>(subplan, subplan->Pos, renameElements, true);
    rename->Props.EnsureAtMostOne = true;

    auto unionAll = std::make_shared<TOpUnionAll>(rename, map, subplan->Pos, true);

    auto limitExpr = ctx.ExprCtx.NewCallable(subplan->Pos, "Uint64", {ctx.ExprCtx.NewAtom(subplan->Pos, "1")});
    auto limit = std::make_shared<TOpLimit>(unionAll, subplan->Pos, limitExpr);

    TVector<std::pair<TInfoUnit,TInfoUnit>> joinKeys;
    auto cross = std::make_shared<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
    unaryOp->Children[0] = cross;

    props.ScalarSubplans.Remove(scalarIU);

    return true;
}

// We push the map operator only below join right now
// We only push a non-projecting map operator, and there are some limitations to where we can push:
//  - we cannot push the right side of left join for example or left side of right join

std::shared_ptr<IOperator> TPushMapRule::SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);
    if (map->Project) {
        return input;
    }

    // Don't handle renames at this point, just expressions that acutally compute something
    for (auto &[var, body] : map->MapElements) {
        if (!std::holds_alternative<TExprNode::TPtr>(body)) {
            return input;
        }
    }

    if (map->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(map->GetInput());
    bool canPushRight = join->JoinKind != "Left" && join->JoinKind != "LeftOnly";
    bool canPushLeft = join->JoinKind != "Right" && join->JoinKind != "RightOnly";

    // Make sure the join and its inputs are single consumer
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return input;
    }

    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> leftMapElements;
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> rightMapElements;
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> topMapElements;

    TVector<int> removeElements;

    for (size_t i = 0; i < map->MapElements.size(); i++) {
        auto mapElement = map->MapElements[i];

        TVector<TInfoUnit> mapElIUs;
        GetAllMembers(std::get<TExprNode::TPtr>(mapElement.second), mapElIUs, props);

        if (!IUSetDiff(mapElIUs, join->GetLeftInput()->GetOutputIUs()).size() && canPushLeft) {
            leftMapElements.push_back(mapElement);
        } else if (!IUSetDiff(mapElIUs, join->GetRightInput()->GetOutputIUs()).size() && canPushRight) {
            rightMapElements.push_back(mapElement);
        } else {
            topMapElements.push_back(mapElement);
        }
    }

    if (!leftMapElements.size() && !rightMapElements.size()) {
        return input;
    }

    std::shared_ptr<IOperator> output;
    if (!topMapElements.size()) {
        output = join;
    } else {
        output = std::make_shared<TOpMap>(join, input->Pos, topMapElements, false);
    }

    if (leftMapElements.size()) {
        auto leftInput = join->GetLeftInput();
        join->Children[0] = std::make_shared<TOpMap>(leftInput, input->Pos, leftMapElements, false);
    }

    if (rightMapElements.size()) {
        auto rightInput = join->GetRightInput();
        join->Children[1] = std::make_shared<TOpMap>(rightInput, input->Pos, rightMapElements, false);
    }

    // If there was an enforcer on the input map, move it to the output
    if (input->Props.OrderEnforcer.has_value()) {
        output->Props.OrderEnforcer = input->Props.OrderEnforcer;
    }

    return output;
}

// FIXME: We currently support pushing filter into Inner, Cross and Left Join
std::shared_ptr<IOperator> TPushFilterRule::SimpleTestAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

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
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
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

    auto conjunctInfo = filter->GetConjunctInfo(props);

    // Check if we need a top level filter
    TVector<TFilterInfo> topLevelPreds;
    TVector<TFilterInfo> pushLeft;
    TVector<TFilterInfo> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (auto f : conjunctInfo.Filters) {
        if (!IUSetDiff(f.FilterIUs, leftIUs).size()) {
            pushLeft.push_back(f);
        } else if (!IUSetDiff(f.FilterIUs, rightIUs).size()) {
            pushRight.push_back(f);
        } else {
            topLevelPreds.push_back(f);
        }
    }

    for (auto c : conjunctInfo.JoinConditions) {
        if (!IUSetDiff({c.LeftIU}, leftIUs).size() && !IUSetDiff({c.RightIU}, rightIUs).size()) {
            joinConditions.push_back(std::make_pair(c.LeftIU, c.RightIU));
        } else if (!IUSetDiff({c.LeftIU}, rightIUs).size() && !IUSetDiff({c.RightIU}, leftIUs).size()) {
            joinConditions.push_back(std::make_pair(c.RightIU, c.LeftIU));
        } else {
            TVector<TInfoUnit> vars = {c.LeftIU};
            vars.push_back(c.RightIU);
            if (!IUSetDiff(vars, leftIUs).size()) {
                pushLeft.push_back(TFilterInfo(c.ConjunctExpr, vars));
            } else {
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
        auto leftLambda = BuildFilterLambdaFromConjuncts(leftInput->Pos, pushLeft, ctx.ExprCtx);
        leftInput = std::make_shared<TOpFilter>(leftInput, input->Pos, leftLambda);
    }

    if (pushRight.size()) {
        if (join->JoinKind == "Left") {
            TVector<TFilterInfo> predicatesForRightSide;
            for (const auto &predicate : pushRight) {
                if (IsNullRejectingPredicate(predicate, ctx.ExprCtx)) {
                    predicatesForRightSide.push_back(predicate);
                } else {
                    topLevelPreds.push_back(predicate);
                }
            }
            if (predicatesForRightSide.size()) {
                auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Pos, pushRight, ctx.ExprCtx);
                rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightLambda);
                join->JoinKind = "Inner";
            } else {
                return input;
            }
        } else {
            auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Pos, pushRight, ctx.ExprCtx);
            rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightLambda);
        }
    }

    if (join->JoinKind == "Cross" && joinConditions.size()) {
        join->JoinKind = "Inner";
    }

    join->Children[0] = leftInput;
    join->Children[1] = rightInput;

    if (topLevelPreds.size()) {
        auto topFilterLambda = BuildFilterLambdaFromConjuncts(join->Pos, topLevelPreds, ctx.ExprCtx);
        output =  std::make_shared<TOpFilter>(join, input->Pos, topFilterLambda);
    } else {
        output = join;
    }

    if (input->Props.OrderEnforcer.has_value()) {
        output->Props.OrderEnforcer = input->Props.OrderEnforcer;
    }

    return output;
}

bool TAssignStagesRule::TestAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    auto nodeName = input->ToString(ctx.ExprCtx);
    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return false;
    }

    for (auto &child : input->Children) {
        if (!child->Props.StageId.has_value()) {
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
            readName = opRead->Alias;
        } else {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;

    } else if (input->Kind == EOperator::Join) {
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
            for (auto key : join->JoinKeys) {
                leftShuffleKeys.push_back(key.first);
                rightShuffleKeys.push_back(key.second);
            }

            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TShuffleConnection>(leftShuffleKeys, isLeftSourceStage));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TShuffleConnection>(rightShuffleKeys, isRightSourceStage));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    } else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = input->Children[0];
        auto prevStageId = *(childOp->Props.StageId);

        // If the child operator is a source, it requires its own stage
        // So we have build a new stage for current operator
        if (childOp->Kind == EOperator::Source) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TSourceConnection>());
        } 
        // If the child operator is not single use, we also need to create a new stage
        // for current operator with a map connection
        else if (!childOp->IsSingleConsumer()) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TMapConnection>(false));
        }
        else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    } else if (input->Kind == EOperator::Limit) {
        auto limit = CastOperator<TOpLimit>(input);
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(limit->GetInput()->Props.StageId);
        auto conn = std::make_shared<TUnionAllConnection>(props.StageGraph.IsSourceStage(prevStageId));
        props.StageGraph.Connect(prevStageId, newStageId,conn);
    } else if (input->Kind == EOperator::UnionAll) {
        auto unionAll = CastOperator<TOpUnionAll>(input);

        auto leftStage = unionAll->GetLeftInput()->Props.StageId;
        auto rightStage = unionAll->GetRightInput()->Props.StageId;

        bool isLeftSourceStage = props.StageGraph.IsSourceStage(*leftStage);
        bool isRightSourceStage = props.StageGraph.IsSourceStage(*rightStage);

        auto newStageId = props.StageGraph.AddStage();
        unionAll->Props.StageId = newStageId;

        props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TUnionAllConnection>(isLeftSourceStage));
        props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TUnionAllConnection>(isRightSourceStage));

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        const bool isInputSourceStage = props.StageGraph.IsSourceStage(inputStageId);
        const auto shuffleKeys = aggregate->KeyColumns.size() ? aggregate->KeyColumns : GetHashableKeys(aggregate->GetInput());

        props.StageGraph.Connect(inputStageId, newStageId, std::make_shared<TShuffleConnection>(shuffleKeys, isInputSourceStage));
        YQL_CLOG(TRACE, CoreDq) << "Assign stage to Aggregation ";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}

TRuleBasedStage RuleStage1 = TRuleBasedStage(
    {
        std::make_shared<TInlineScalarSubplanRule>(),
        std::make_shared<TExtractJoinExpressionsRule>(), 
        std::make_shared<TPushMapRule>(), 
        std::make_shared<TPushFilterRule>()
    });
TRuleBasedStage RuleStage2 = TRuleBasedStage({std::make_shared<TAssignStagesRule>()});

} // namespace NKqp
} // namespace NKikimr