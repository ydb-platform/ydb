#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>

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

TExprNode::TPtr BuildFilterLambdaFromConjuncts(TPositionHandle pos, TVector<TFilterInfo> conjuncts, TExprContext &ctx, bool pgSyntax) {
    auto arg = Build<TCoArgument>(ctx, pos).Name("lambda_arg").Done();
    TExprNode::TPtr lambda;

    if (conjuncts.size() == 1) {
        auto filterInfo = conjuncts[0];
        auto body = ReplaceArg(filterInfo.FilterBody, arg.Ptr(), ctx);
        if (pgSyntax && !filterInfo.FromPg) {
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
            if (pgSyntax && !c.FromPg) {
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

const THashSet<TString> CmpOperators{"=", "<", ">", "<=", ">="};

TExprNode::TPtr PruneCast(TExprNode::TPtr node) {
    if (node->IsCallable("ToPg") || node->IsCallable("PgCast")) {
        return node->Child(0);
    }
    return node;
}

[[maybe_unused]]
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

void UpdateNumOfConsumers(std::shared_ptr<IOperator> &input) {
    auto &props = input->Props;
    if (props.NumOfConsumers.has_value()) {
        props.NumOfConsumers.value() += 1;
    } else {
        props.NumOfConsumers = 1;
    }
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

std::shared_ptr<TOpCBOTree> JoinCBOTrees(std::shared_ptr<TOpCBOTree> & left, std::shared_ptr<TOpCBOTree> & right, std::shared_ptr<TOpJoin> &join) {
    auto newJoin = std::make_shared<TOpJoin>(left->TreeRoot, right->TreeRoot, join->Pos, join->JoinKind, join->JoinKeys);

    auto treeNodes = left->TreeNodes;
    treeNodes.insert(treeNodes.end(), right->TreeNodes.begin(), right->TreeNodes.end());
    treeNodes.push_back(newJoin);

    return std::make_shared<TOpCBOTree>(newJoin, treeNodes, newJoin->Pos);
}

std::shared_ptr<TOpCBOTree> AddJoinToCBOTree(std::shared_ptr<TOpCBOTree> & cboTree, std::shared_ptr<TOpJoin> &join) {
    TVector<std::shared_ptr<IOperator>> treeNodes;

    if (join->GetLeftInput() == cboTree) {
        join->SetLeftInput(cboTree->TreeRoot);
        treeNodes.insert(treeNodes.end(), cboTree->TreeNodes.begin(), cboTree->TreeNodes.end());
        treeNodes.push_back(join);
    }
    else {
        join->SetRightInput(cboTree->TreeRoot);
        treeNodes.insert(treeNodes.end(), cboTree->TreeNodes.begin(), cboTree->TreeNodes.end());
        treeNodes.push_back(join);
    }

    return std::make_shared<TOpCBOTree>(join, treeNodes, join->Pos);
}

void ExtractConjuncts(TExprNode::TPtr node, TVector<TExprNode::TPtr> & conjuncts) {
    if (TCoAnd::Match(node.Get())) {
        for (auto c : node->ChildrenList()) {
            conjuncts.push_back(c);
        }
    }
    else {
        conjuncts.push_back(node);
    }
}

std::shared_ptr<TOpFilter> FuseFilters(const std::shared_ptr<TOpFilter>& top, const std::shared_ptr<TOpFilter>& bottom, TExprContext &ctx) {
    auto topLambda = TCoLambda(top->FilterLambda);
    auto bottomLambda = TCoLambda(bottom->FilterLambda);
    auto arg = Build<TCoArgument>(ctx, top->Pos).Name("lambda_arg").Done();

    TVector<TExprNode::TPtr> newConjuncts;
    ExtractConjuncts(ReplaceArg(topLambda.Body().Ptr(), arg.Ptr(), ctx), newConjuncts);
    ExtractConjuncts(ReplaceArg(bottomLambda.Body().Ptr(), arg.Ptr(), ctx), newConjuncts);

    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx, top->Pos)
        .Args(arg)
        .Body<TCoAnd>()
            .Add(newConjuncts)
        .Build()
    .Done().Ptr();
    // clang-format on

    return make_shared<TOpFilter>(bottom->GetInput(), top->Pos, newLambda);
}

} // namespace

namespace NKikimr {
namespace NKqp {

// Remove extra maps that arrise during translation
// Identity map that doesn't project can always be removed
// Identity map that projects maybe a projection operator and can be removed if it doesn't do any extra
// projections

std::shared_ptr<IOperator> TRemoveIdenityMapRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(input);

    /***
     * If its a project map, check that it output the same number of columns as the operator below
     */

    if (map->Project && map->GetOutputIUs().size() != map->GetInput()->GetOutputIUs().size()) {
        return input;
    }

    for (const auto& mapElement : map->MapElements) {
        if (!mapElement.IsRename()) {
            return input;
        }
        auto fromColumn = mapElement.GetRename();
        if (fromColumn != mapElement.GetElementName()) {
            return input;
        }
    }

    return map->GetInput();
}

// Currently we only extract simple expressions where there is only one variable on either side

bool TExtractJoinExpressionsRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
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

            TExprNode::TPtr leftSide;
            TExprNode::TPtr rightSide;

            if (TestAndExtractEqualityPredicate(predicate, leftSide, rightSide)) {

                if (leftSide->IsCallable("Member") && rightSide->IsCallable("Member")) {
                    continue;
                }

                TVector<TInfoUnit> leftIUs;
                TVector<TInfoUnit> rightIUs;
                GetAllMembers(leftSide, leftIUs, props, false, true);
                GetAllMembers(rightSide, rightIUs, props, false, true);

                if (leftIUs.size() == 1 && rightIUs.size() == 1) {
                    matchedConjuncts.push_back(std::make_pair(idx, f.FilterBody));
                }
            }
        }
    }

    if (matchedConjuncts.size()) {
        TVector<TMapElement> mapElements;
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

                mapElements.emplace_back(TInfoUnit(newName), mapLambda);

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

                mapElements.emplace_back(TInfoUnit(newName), mapLambda);

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
        filter->SetInput(newMap);
        return true;
    }

    return false;
}

// Rewrite a single scalar subplan into a cross-join
bool TInlineScalarSubplanRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    auto subplanIUs = input->GetSubplanIUs(props);
    TVector<TInfoUnit> scalarIUs;
    for (const auto& iu : subplanIUs) {
        auto subplanEntry = props.Subplans.PlanMap.at(iu);
        if (subplanEntry.Type == ESubplanType::EXPR) {
            scalarIUs.push_back(iu);
            break;
        }
    }

    if (scalarIUs.empty()) {
        return false;
    }

    auto scalarIU = scalarIUs[0];
    auto subplan = props.Subplans.PlanMap.at(scalarIU).Plan;
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

    TVector<TMapElement> mapElements;
    mapElements.emplace_back(scalarIU, mapLambda);
    auto map = std::make_shared<TOpMap>(emptySource, subplan->Pos, mapElements, true);

    TVector<TMapElement> renameElements;
    renameElements.emplace_back(scalarIU, subplanResIU);
    auto rename = std::make_shared<TOpMap>(subplan, subplan->Pos, renameElements, true);
    rename->Props.EnsureAtMostOne = true;

    auto unionAll = std::make_shared<TOpUnionAll>(rename, map, subplan->Pos, true);

    auto limitExpr = ctx.ExprCtx.NewCallable(subplan->Pos, "Uint64", {ctx.ExprCtx.NewAtom(subplan->Pos, "1")});
    auto limit = std::make_shared<TOpLimit>(unionAll, subplan->Pos, limitExpr);

    TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
    auto cross = std::make_shared<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
    unaryOp->SetInput(cross);

    props.Subplans.Remove(scalarIU);

    return true;
}

std::shared_ptr<IOperator> TInlineSimpleInExistsSubplanRule::SimpleMatchAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter || props.PgSyntax) {
        return input;
    }

    // Check that the filter lambda is a conjunction of one or more elements
    auto filter = CastOperator<TOpFilter>(input);
    auto lambdaBody = filter->FilterLambda->ChildPtr(1);

    if (!TCoAnd::Match(lambdaBody.Get()) && !TCoNot::Match(lambdaBody.Get()) && !TCoMember::Match(lambdaBody.Get())) {
        return input;
    }

    // Decompose the conjunction into individual conjuncts
    TVector<TExprNode::TPtr> conjuncts;
    if (TCoAnd::Match(lambdaBody.Get())) {
        for (const auto& child : lambdaBody->Children()) {
            conjuncts.push_back(child);
        }
    } else {
        conjuncts.push_back(lambdaBody);
    }

    // Find the first conjunct that is a simple in or exists subplan
    bool negated = false;
    TInfoUnit iu;
    TSubplanEntry subplan;
    size_t conjunctIdx;

    for (conjunctIdx = 0; conjunctIdx < conjuncts.size(); conjunctIdx++) {
        auto maybeSubplan = conjuncts[conjunctIdx];

        if (TCoNot::Match(maybeSubplan.Get())) {
            maybeSubplan = maybeSubplan->ChildPtr(0);
            negated = true;
        }
        if (TCoMember::Match(maybeSubplan.Get())) {
            auto name = TString(maybeSubplan->Child(1)->Content());
            iu = TInfoUnit(name);
            if (props.Subplans.PlanMap.contains(iu)) {
                subplan = props.Subplans.PlanMap.at(iu);
                if (subplan.Type == ESubplanType::IN_SUBPLAN || subplan.Type == ESubplanType::EXISTS) {
                    break;
                }
            }
        }
    }

    if (conjunctIdx == conjuncts.size()) {
        return input;
    }

    std::shared_ptr<IOperator> join;

    // We build a semi-join or a left-only join when processing IN subplan
    if (subplan.Type == ESubplanType::IN_SUBPLAN) {
        auto leftJoinInput = filter->GetInput();
        auto joinKind = negated ? "LeftOnly" : "LeftSemi";

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        auto planIUs = subplan.Plan->GetOutputIUs();
        YQL_CLOG(TRACE, CoreDq) << "In tuple size: " << subplan.Tuple.size() << ", subplan tuple size: " << planIUs.size();

        Y_ENSURE(subplan.Tuple.size() == planIUs.size());

        for (size_t i = 0; i < planIUs.size(); i++) {
            joinKeys.push_back(std::make_pair(subplan.Tuple[i], planIUs[i]));
        }

        join = std::make_shared<TOpJoin>(leftJoinInput, subplan.Plan, input->Pos, joinKind, joinKeys);
        conjuncts.erase(conjuncts.begin() + conjunctIdx);
    }
    // EXISTS and NOT EXISTS
    else {
        auto one = ctx.ExprCtx.NewCallable(filter->Pos, "Uint64", {ctx.ExprCtx.NewAtom(filter->Pos, "1")});
        auto zero = ctx.ExprCtx.NewCallable(filter->Pos, "Uint64", {ctx.ExprCtx.NewAtom(filter->Pos, "0")});
        auto limit = std::make_shared<TOpLimit>(subplan.Plan, filter->Pos, one);
        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;

        // clang-format off
        auto zeroLambda = Build<TCoLambda>(ctx.ExprCtx, filter->Pos)
            .Args({"arg"})
            .Body(zero)
        .Done().Ptr();
        // clang-format on

        countMapElements.emplace_back(countResult, zeroLambda);
        auto countMap = std::make_shared<TOpMap>(limit, filter->Pos, countMapElements, true);
        TOpAggregationTraits aggFunction(countResult, "count");
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = std::make_shared<TOpAggregate>(countMap, aggs, keyColumns, EAggregationPhase::Final, false, filter->Pos);
        const TString compareCallable = negated ? "==" : "!=";

        auto arg = Build<TCoArgument>(ctx.ExprCtx, filter->Pos).Name("lambda_arg").Done();
        // clang-format off
        auto member = Build<TCoMember>(ctx.ExprCtx, filter->Pos)
            .Struct(arg)
            .Name().Value(countResult.GetFullName()).Build()
        .Done().Ptr();
        // clang-format on

        auto body = ctx.ExprCtx.NewCallable(filter->Pos, compareCallable, {member, zero});

        // clang-format off
        auto lambda = Build<TCoLambda>(ctx.ExprCtx, filter->Pos)
            .Args({arg})
            .Body(body)
        .Done().Ptr();
        // clang-format on

        TVector<TMapElement> mapElements;
        auto compareResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        mapElements.emplace_back(compareResult, lambda);
        auto map = std::make_shared<TOpMap>(agg, filter->Pos, mapElements, true);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        join = std::make_shared<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);

        // clang-format off
        auto resultMember = Build<TCoMember>(ctx.ExprCtx, filter->Pos)
            .Struct(arg)
            .Name().Value(compareResult.GetFullName()).Build()
        .Done().Ptr();
        // clang-format on

        conjuncts[conjunctIdx] = resultMember;
    }

    props.Subplans.Remove(iu);
    // If there was a single conjunct, we can get rid of the filter completely
    if (conjuncts.empty()) {
        return join;
    }

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    auto arg = Build<TCoArgument>(ctx.ExprCtx, input->Pos).Name("lambda_arg").Done().Ptr();
    TExprNode::TPtr newLambdaBody;
    if (conjuncts.size() == 1) {
        newLambdaBody = conjuncts[0];
    } else {
        newLambdaBody = Build<TCoAnd>(ctx.ExprCtx, input->Pos).Add(conjuncts).Done().Ptr();
    }
    newLambdaBody = ReplaceArg(newLambdaBody, arg, ctx.ExprCtx);

    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx.ExprCtx, input->Pos)
        .Args({arg})
        .Body(newLambdaBody)
    .Done().Ptr();
    // clang-format on

    return std::make_shared<TOpFilter>(join, filter->Pos, newLambda);
}

// We push the map operator only below join right now
// We only push a non-projecting map operator, and there are some limitations to where we can push:
//  - we cannot push the right side of left join for example or left side of right join

std::shared_ptr<IOperator> TPushMapRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
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
    for (const auto &mapElement : map->MapElements) {
        if (!mapElement.IsExpression()) {
            return input;
        }
    }

    if (map->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(map->GetInput());
    bool canPushRight = join->JoinKind != "Left" && join->JoinKind != "LeftOnly";
    bool canPushLeft = join->JoinKind != "Right" && join->JoinKind != "RightOnly";

    // Make sure the join is single consumer
    if (!join->IsSingleConsumer()) {
        return input;
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<TMapElement> topMapElements;
    TVector<int> removeElements;

    for (size_t i = 0; i < map->MapElements.size(); i++) {
        auto mapElement = map->MapElements[i];

        TVector<TInfoUnit> mapElIUs;
        auto expression = mapElement.GetExpression();
        GetAllMembers(expression, mapElIUs, props, false, true);

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
        join->SetLeftInput(std::make_shared<TOpMap>(leftInput, input->Pos, leftMapElements, false));
    }

    if (rightMapElements.size()) {
        auto rightInput = join->GetRightInput();
        join->SetRightInput(std::make_shared<TOpMap>(rightInput, input->Pos, rightMapElements, false));
    }

    return output;
}

std::shared_ptr<IOperator> TPushLimitIntoSortRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Limit) {
        return input;
    }

    auto limit = CastOperator<TOpLimit>(input);
    if (limit->GetInput()->Kind != EOperator::Sort) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(limit->GetInput());
    if (sort->LimitCond) {
        return input;
    }

    sort->LimitCond = limit->LimitCond;
    return sort;
}

// FIXME: We currently support pushing filter into Inner, Cross and Left Join
std::shared_ptr<IOperator> TPushFilterRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

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

    // Make sure the join is single consumer
    if (!join->IsSingleConsumer()) {
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
    const auto conjunctInfo = filter->GetConjunctInfo(props);

    // Check if we need a top level filter
    TVector<TFilterInfo> topLevelPreds;
    TVector<TFilterInfo> pushLeft;
    TVector<TFilterInfo> pushRight;
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinConditions;

    for (const auto& filter : conjunctInfo.Filters) {
        if (!IUSetDiff(filter.FilterIUs, leftIUs).size()) {
            pushLeft.push_back(filter);
        } else if (!IUSetDiff(filter.FilterIUs, rightIUs).size()) {
            pushRight.push_back(filter);
        } else {
            topLevelPreds.push_back(filter);
        }
    }

    for (const auto& condition : conjunctInfo.JoinConditions) {
        if (!IUSetDiff({condition.LeftIU}, leftIUs).size() && !IUSetDiff({condition.RightIU}, rightIUs).size()) {
            joinConditions.push_back(std::make_pair(condition.LeftIU, condition.RightIU));
        } else if (!IUSetDiff({condition.LeftIU}, rightIUs).size() && !IUSetDiff({condition.RightIU}, leftIUs).size()) {
            joinConditions.push_back(std::make_pair(condition.RightIU, condition.LeftIU));
        } else {
            TVector<TInfoUnit> vars{condition.LeftIU, condition.RightIU};
            if (!IUSetDiff(vars, leftIUs).size()) {
                pushLeft.push_back(TFilterInfo(condition.ConjunctExpr, vars));
            } else {
                pushRight.push_back(TFilterInfo(condition.ConjunctExpr, vars));
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
        auto leftLambda = BuildFilterLambdaFromConjuncts(leftInput->Pos, pushLeft, ctx.ExprCtx, props.PgSyntax);
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
                auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Pos, pushRight, ctx.ExprCtx, props.PgSyntax);
                rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightLambda);
                join->JoinKind = "Inner";
            } else {
                return input;
            }
        } else {
            auto rightLambda = BuildFilterLambdaFromConjuncts(rightInput->Pos, pushRight, ctx.ExprCtx, props.PgSyntax);
            rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightLambda);
        }
    }

    if (join->JoinKind == "Cross" && joinConditions.size()) {
        join->JoinKind = "Inner";
    }

    join->SetLeftInput(leftInput);
    join->SetRightInput(rightInput);

    if (topLevelPreds.size()) {
        auto topFilterLambda = BuildFilterLambdaFromConjuncts(join->Pos, topLevelPreds, ctx.ExprCtx, props.PgSyntax);
        output =  std::make_shared<TOpFilter>(join, input->Pos, topFilterLambda);
    } else {
        output = join;
    }

    return output;
}

bool IsSuitableToApplyPeephole(const std::shared_ptr<IOperator>& input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto lambda = TCoLambda(filter->FilterLambda);
    auto peepholeIsNeeded = [&](const TExprNode::TPtr& node) -> bool {
        // Here is a list of Callables for which peephole is needed.
        if (node->IsCallable({"SqlIn"})) {
            return true;
        }
        return false;
    };

    return !!FindNode(lambda.Body().Ptr(), peepholeIsNeeded);
}

std::shared_ptr<IOperator> TPeepholePredicate::SimpleMatchAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    if (!IsSuitableToApplyPeephole(input)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto lambda = TCoLambda(filter->FilterLambda);
    TVector<const TTypeAnnotationNode*> argTypes{lambda.Args().Arg(0).Ptr()->GetTypeAnn()};
    // Closure an original predicate, we cannot call `Peephole` for free args.
    // clang-format off
    auto predicateClosure = Build<TKqpPredicateClosure>(ctx.ExprCtx, input->Pos)
        .Lambda<TCoLambda>()
            .Args({"arg"})
            .Body<TExprApplier>()
                .Apply(lambda)
                .With(lambda.Args().Arg(0), "arg")
            .Build()
        .Build()
        .ArgsType(ExpandType(input->Pos, *ctx.ExprCtx.MakeType<TTupleExprType>(argTypes), ctx.ExprCtx))
    .Done();
    // clang-format on
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Before peephole: " << KqpExprToPrettyString(predicateClosure, ctx.ExprCtx);

    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    // Using a special PeepholeTypeAnnTransformer.
    if (const auto status = PeepHoleOptimizeNode(predicateClosure.Ptr(), afterPeephole, ctx.ExprCtx, ctx.TypeCtx, ctx.PeepholeTypeAnnTransformer.Get(),
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO] Peephole failed with status: " << status << Endl;
        return input;
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] After peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx.ExprCtx);

    auto lambdaAfterPeephole = TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda();
    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx.ExprCtx, input->Pos)
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(lambdaAfterPeephole.Body())
            .With(lambdaAfterPeephole.Args().Arg(0), "arg")
        .Build()
    .Done().Ptr();
    // clang-format on

    return std::make_shared<TOpFilter>(filter->GetInput(), input->Pos, newLambda);
}

bool IsSuitableToPushPredicateToColumnTables(const std::shared_ptr<IOperator>& input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput();
    return ((maybeRead->Kind == EOperator::Source) && (CastOperator<TOpRead>(maybeRead)->GetTableStorageType() == NYql::EStorageType::ColumnStorage) &&
            filter->GetTypeAnn());
}

std::shared_ptr<IOperator> TPushOlapFilterRule::SimpleMatchAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(props);
    if (!ctx.KqpCtx.Config->HasOptEnableOlapPushdown()) {
        return input;
    }

    const TPushdownOptions pushdownOptions(ctx.KqpCtx.Config->GetEnableOlapScalarApply(), ctx.KqpCtx.Config->GetEnableOlapSubstringPushdown(),
                                           /*StripAliasPrefixForColumnName=*/true);
    if (!IsSuitableToPushPredicateToColumnTables(input)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput());
    const auto lambda = TCoLambda(filter->FilterLambda);
    const auto& lambdaArg = lambda.Args().Arg(0).Ref();
    TExprBase predicate = lambda.Body();

    TOLAPPredicateNode predicateTree;
    predicateTree.ExprNode = predicate.Ptr();
    CollectPredicates(predicate, predicateTree, &lambdaArg, filter->GetTypeAnn()->Cast<TListExprType>()->GetItemType(), pushdownOptions);
    YQL_ENSURE(predicateTree.IsValid(), "Collected OLAP predicates are invalid");
    TPositionHandle pos = input->Pos;

    auto [pushable, remaining] = SplitForPartialPushdown(predicateTree, false);
    TVector<TFilterOpsLevels> pushedPredicates;
    for (const auto& p : pushable) {
        pushedPredicates.emplace_back(PredicatePushdown(TExprBase(p.ExprNode), lambdaArg, ctx.ExprCtx, pos, pushdownOptions));
    }

    // TODO: All or nothing currently. Add partial pushdown.
    if (pushedPredicates.empty() || !remaining.empty()) {
        return input;
    }

    const auto& pushedFilter = TFilterOpsLevels::Merge(pushedPredicates, ctx.ExprCtx, pos);
    const auto remainingFilter = CombinePredicatesWithAnd(remaining, ctx.ExprCtx, pos, false, true);

    TMaybeNode<TExprBase> olapFilter;
    if (pushedFilter.FirstLevelOps.IsValid()) {
        // clang-format off
        olapFilter = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
            .Input(lambda.Args().Arg(0))
            .Condition(pushedFilter.FirstLevelOps.Cast())
        .Done();
        // clang-format on
    }

    if (pushedFilter.SecondLevelOps.IsValid()) {
        // clang-format off
        olapFilter = Build<TKqpOlapFilter>(ctx.ExprCtx, pos)
            .Input(olapFilter.IsValid() ? olapFilter.Cast() : lambda.Args().Arg(0))
            .Condition(pushedFilter.SecondLevelOps.Cast())
        .Done();
        // clang-format on
    }

    if (!olapFilter.IsValid()) {
        YQL_CLOG(TRACE, ProviderKqp) << "KqpOlapFilter was not constructed";
        return input;
    }

    // clang-format off
    auto newOlapFilterLambda = Build<TCoLambda>(ctx.ExprCtx, pos)
        .Args({"olap_filter_row"})
        .Body<TExprApplier>()
            .Apply(olapFilter.Cast())
            .With(lambda.Args().Arg(0), "olap_filter_row")
            .Build()
        .Done();
    // clang-format on
    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP lambda: " << KqpExprToPrettyString(newOlapFilterLambda, ctx.ExprCtx);

    return std::make_shared<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, newOlapFilterLambda.Ptr(),
                                     read->Pos);
}

/**
 * Initially we build CBO only for joins that don't have other joins or CBO trees as arguments
 * There could be an intermediate filter in between, we also check that
 */
std::shared_ptr<IOperator> TBuildInitialCBOTreeRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto containsJoins = [](const std::shared_ptr<IOperator>& op) {
        std::shared_ptr<IOperator> maybeJoin = op;
        if (op->Kind == EOperator::Filter) {
            maybeJoin = CastOperator<TOpFilter>(op)->GetInput();
        }
        return (maybeJoin->Kind == EOperator::Join || maybeJoin->Kind == EOperator::CBOTree);
    };

    if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        if (!containsJoins(join->GetLeftInput()) && !containsJoins(join->GetRightInput())) {
            return std::make_shared<TOpCBOTree>(input, input->Pos);
        }
    }

    return input;
}

/**
 * Expanding CBO tree is more tricky:
 *  - We can have a join that joins a CBOtree with something else, and there could be a filter in between that we
 *    would like to push out
 *  - We need to extend this to support filter and aggregates that will be later supported by DP CBO
 * FIXME: Add maybes to make matching look simpler
 * FIXME: Support other joins for filter push-out, refactor into a lambda to apply to both sides
 */
std::shared_ptr<IOperator> TExpandCBOTreeRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    // In case there is a join of a CBO tree (maybe with a filter stuck in-between)
    // we push this join into the CBO tree and push the filter out above

    if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftInput = join->GetLeftInput();
        auto rightInput = join->GetRightInput();

        std::shared_ptr<TOpFilter> maybeFilter;
        std::shared_ptr<TOpCBOTree> cboTree;

        bool leftSideCBOTree = true;

        auto findCBOTree = [&join](const std::shared_ptr<IOperator>& op,
                std::shared_ptr<TOpCBOTree>& cboTree,
                std::shared_ptr<TOpFilter>& maybeFilter) {

            if (op->Kind == EOperator::CBOTree) {
                cboTree = CastOperator<TOpCBOTree>(op);
                return true;
            }
            if (op->Kind == EOperator::Filter &&
                    CastOperator<TOpFilter>(op)->GetInput()->Kind == EOperator::CBOTree &&
                    join->JoinKind == "Inner") {

                maybeFilter = CastOperator<TOpFilter>(op);
                cboTree = CastOperator<TOpCBOTree>(maybeFilter->GetInput());
                return true;
            }

            return false;
        };

        if (!findCBOTree(leftInput, cboTree, maybeFilter)) {
            if (!findCBOTree(rightInput, cboTree, maybeFilter)) {
                return input;
            } else {
                leftSideCBOTree = false;
            }
        }

        std::shared_ptr<TOpFilter> maybeAnotherFilter;
        auto otherSide = leftSideCBOTree ? join->GetRightInput() : join->GetLeftInput();
        std::shared_ptr<TOpCBOTree> otherSideCBOTree;

        if (otherSide->Kind == EOperator::Filter &&
                CastOperator<TOpFilter>(otherSide)->GetInput()->Kind == EOperator::CBOTree &&
                join->JoinKind == "Inner") {

            maybeAnotherFilter = CastOperator<TOpFilter>(otherSide);
            otherSideCBOTree = CastOperator<TOpCBOTree>(maybeAnotherFilter->GetInput());
        }

        if (otherSideCBOTree) {
            if (leftSideCBOTree) {
                cboTree = JoinCBOTrees(cboTree, otherSideCBOTree, join);
            } else {
                cboTree = JoinCBOTrees(otherSideCBOTree, cboTree, join);
            }
        } else {
            cboTree = AddJoinToCBOTree(cboTree, join);
        }

        if (maybeFilter && maybeAnotherFilter) {
            maybeFilter = FuseFilters(maybeFilter, maybeAnotherFilter, ctx.ExprCtx);
        } else if (maybeAnotherFilter) {
            maybeFilter = maybeAnotherFilter;
        }

        if (maybeFilter) {
            maybeFilter->SetInput(cboTree);
            return maybeFilter;
        } else {
            return cboTree;
        }
    }

    return input;
}

/**
 * Convert unoptimized CBOTrees back into normal operators
 */
std::shared_ptr<IOperator> TInlineCBOTreeRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind == EOperator::CBOTree) {
        auto cboTree = CastOperator<TOpCBOTree>(input);
        return cboTree->TreeRoot;
    }

    return input;
}

/**
 * Assign stages and build stage graph in the process
 */
bool TAssignStagesRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    auto nodeName = input->ToString(ctx.ExprCtx);
    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return false;
    }

    for (const auto& child : input->Children) {
        if (!child->Props.StageId.has_value()) {
            YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " child with unassigned stage";
            return false;
        }
    }

    if (input->Kind == EOperator::EmptySource || input->Kind == EOperator::Source) {
        auto opRead = CastOperator<TOpRead>(input);
        TString readName;
        if (input->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(input);
            auto newStageId = props.StageGraph.AddSourceStage(opRead->Columns, opRead->GetOutputIUs(), opRead->StorageType, opRead->NeedsMap());
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

        const auto leftInputStorageType = props.StageGraph.GetStorageType(*leftStage);
        const auto rightInputStorageType = props.StageGraph.GetStorageType(*rightStage);

        // For cross-join we build a stage with map and broadcast connections
        if (join->JoinKind == "Cross") {
            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TMapConnection>(leftInputStorageType));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TBroadcastConnection>(rightInputStorageType));
        }

        // For inner join (we don't support other joins yet) we build a new stage
        // with GraceJoinCore and connect inputs via Shuffle connections
        else {
            TVector<TInfoUnit> leftShuffleKeys;
            TVector<TInfoUnit> rightShuffleKeys;
            for (const auto& key : join->JoinKeys) {
                leftShuffleKeys.push_back(key.first);
                rightShuffleKeys.push_back(key.second);
            }

            props.StageGraph.Connect(*leftStage, newStageId, std::make_shared<TShuffleConnection>(leftShuffleKeys, leftInputStorageType));
            props.StageGraph.Connect(*rightStage, newStageId, std::make_shared<TShuffleConnection>(rightShuffleKeys, rightInputStorageType));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    } else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = CastOperator<IUnaryOperator>(input)->GetInput();
        auto prevStageId = *(childOp->Props.StageId);
        UpdateNumOfConsumers(childOp);

        // If the child operator is a source, it requires its own stage
        // So we have build a new stage for current operator
        if (childOp->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(childOp);
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            std::shared_ptr<TConnection> connection;
            // Type of connections depends on the storage type.
            switch (opRead->GetTableStorageType()) {
                case NYql::EStorageType::RowStorage: {
                    connection.reset(new TSourceConnection());
                    break;
                }
                case NYql::EStorageType::ColumnStorage: {
                    connection.reset(new TUnionAllConnection(NYql::EStorageType::ColumnStorage, props.StageGraph.GetOutputIndex(prevStageId)));
                    break;
                }
                default: {
                    Y_ENSURE(false, "Invalid storage type for op read");
                    break;
                }
            }
            props.StageGraph.Connect(prevStageId, newStageId, connection);
        }
        // If the child operator is not single use, we also need to create a new stage
        // for current operator with a map connection
        else if (!childOp->IsSingleConsumer()) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, std::make_shared<TMapConnection>());
        } else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    } else if (input->Kind == EOperator::Sort) {
        auto sort = CastOperator<TOpSort>(input);
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(sort->GetInput()->Props.StageId);
        auto conn = std::make_shared<TUnionAllConnection>(props.StageGraph.GetStorageType(prevStageId));
        props.StageGraph.Connect(prevStageId, newStageId,conn);
    }
    else if (input->Kind == EOperator::Limit) {
        auto limit = CastOperator<TOpLimit>(input);
        auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        auto prevStageId = *(limit->GetInput()->Props.StageId);
        auto conn = std::make_shared<TUnionAllConnection>(props.StageGraph.GetStorageType(prevStageId));
        props.StageGraph.Connect(prevStageId, newStageId,conn);
    } else if (input->Kind == EOperator::UnionAll) {
        auto unionAll = CastOperator<TOpUnionAll>(input);
        UpdateNumOfConsumers(unionAll->GetLeftInput());
        UpdateNumOfConsumers(unionAll->GetRightInput());

        auto leftStage = unionAll->GetLeftInput()->Props.StageId;
        auto rightStage = unionAll->GetRightInput()->Props.StageId;

        auto newStageId = props.StageGraph.AddStage();
        unionAll->Props.StageId = newStageId;

        props.StageGraph.Connect(
            *leftStage, newStageId,
            std::make_shared<TUnionAllConnection>(props.StageGraph.GetStorageType(*leftStage), props.StageGraph.GetOutputIndex(*leftStage)));
        props.StageGraph.Connect(
            *rightStage, newStageId,
            std::make_shared<TUnionAllConnection>(props.StageGraph.GetStorageType(*rightStage), props.StageGraph.GetOutputIndex(*rightStage)));

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (!aggregate->KeyColumns.empty()) {
            props.StageGraph.Connect(inputStageId, newStageId,
                                     std::make_shared<TShuffleConnection>(aggregate->KeyColumns, props.StageGraph.GetStorageType(inputStageId)));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, std::make_shared<TUnionAllConnection>(props.StageGraph.GetStorageType(inputStageId)));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to Aggregation ";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}

} // namespace NKqp
} // namespace NKikimr