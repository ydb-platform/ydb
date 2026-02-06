#include "kqp_rbo_rules.h"
#include "kqp_operator.h"
#include "kqp_rbo_utils.h"
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

std::shared_ptr<TOpFilter> FuseFilters(const std::shared_ptr<TOpFilter>& top, const std::shared_ptr<TOpFilter>& bottom, bool pgSyntax) {
    TVector<TExpression> conjuncts = top->FilterExpr.SplitConjunct();
    TVector<TExpression> bottomConjuncts = bottom->FilterExpr.SplitConjunct();
    conjuncts.insert(conjuncts.begin(), bottomConjuncts.begin(), bottomConjuncts.end());

    return make_shared<TOpFilter>(bottom->GetInput(), top->Pos, MakeConjunct(conjuncts, pgSyntax));
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

// Pull up correlated filter inside a subplan
// We match the parent of the filter, currently we support only Map and Aggregate

bool TPullUpCorrelatedFilterRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map && input->Kind != EOperator::Aggregate) {
        return false;
    }

    auto unaryOp = CastOperator<IUnaryOperator>(input);

    if (unaryOp->GetInput()->Kind != EOperator::Filter || !unaryOp->GetInput()->IsSingleConsumer()) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(unaryOp->GetInput());

    if (filter->GetInput()->Kind != EOperator::AddDependencies || !filter->GetInput()->IsSingleConsumer()) {
        return false;
    }

    auto deps = CastOperator<TOpAddDependencies>(filter->GetInput());

    auto conjuncts = filter->FilterExpr.SplitConjunct();
    TVector<TExpression> joinConditions;
    TVector<TExpression> otherFilters;
    for (auto const & conj : conjuncts) {
        if (conj.MaybeJoinCondition()) {
            joinConditions.push_back(conj);
        } else {
            otherFilters.push_back(conj);
        }
    }
    
    if (joinConditions.empty()) {
        return false;
    }

    // Select a subset of join conditions that cover all dependencies
    TVector<TExpression> dependentSubset;
    TVector<TExpression> remainderSubset;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> correlated;

    for (const auto & jc : joinConditions) {
        TJoinCondition cond(jc);
        if (std::find(deps->Dependencies.begin(), deps->Dependencies.end(), cond.GetLeftIU()) != deps->Dependencies.end()) {
            dependentSubset.push_back(jc);
            correlated.insert(cond.GetRightIU());
        }
        else if (std::find(deps->Dependencies.begin(), deps->Dependencies.end(), cond.GetRightIU()) != deps->Dependencies.end()) {
            dependentSubset.push_back(jc);
            correlated.insert(cond.GetLeftIU());
        } else {
            remainderSubset.push_back(jc);
        }
    }

    // Make sure all dependencies are covered in this filter
    // FIXME: This is a current limitation
    if (correlated.size() != deps->Dependencies.size()) {
        return false;
    }

    // Split the predicate into a remaining and new part with only dependent conditions
    std::shared_ptr<IOperator> remainingFilter = deps->GetInput();
    if (!otherFilters.empty() || !remainderSubset.empty()) {
        auto newConjuncts = otherFilters;
        newConjuncts.insert(newConjuncts.end(), remainderSubset.begin(), remainderSubset.end());
        auto expr = MakeConjunct(newConjuncts, props.PgSyntax);
        remainingFilter = std::make_shared<TOpFilter>(deps->GetInput(), remainingFilter->Pos, expr);
    }

    auto newExpr = MakeConjunct(dependentSubset, props.PgSyntax);

    if (input->Kind == EOperator::Map) {
        auto map = CastOperator<TOpMap>(input);

        // Stop if we compute something from one of the dependent columns, except for Just
        for (const auto & mapEl : map->MapElements) {
            for (const auto & iu : mapEl.GetExpression().GetInputIUs(false, true)) {
                if (correlated.contains(iu)) {
                    if (!mapEl.IsRename() && !mapEl.GetExpression().IsSingleCallable({"Just"})) {
                        return false;
                    }
                }
            }
        }

        TVector<TInfoUnit> addToMap;

        // Add all dependend columns to projecting map output
        auto mapOutput = map->GetOutputIUs();
        for (const auto & iu : correlated) {
            if (std::find(mapOutput.begin(), mapOutput.end(), iu) == mapOutput.end()) {
                addToMap.push_back(iu);
            }
        }

        // First we add needed columns to the map, if its a projection map
        if (!addToMap.empty() && map->Project) {
            for (const auto & add : addToMap) {
                map->MapElements.push_back(TMapElement(add, add, map->Pos, &ctx.ExprCtx, &props));
            }
        }

        filter->FilterExpr = newExpr;
        map->SetInput(remainingFilter);
        deps->SetInput(map);
        input = filter;

        return true;

    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);

        // Add all missing dependent columns to the group by list
        for (const auto & corr : correlated) {
            if (std::find(aggregate->KeyColumns.begin(), aggregate->KeyColumns.end(), corr) == aggregate->KeyColumns.end()) {
                aggregate->KeyColumns.push_back(corr);
            }
        }

        filter->FilterExpr = newExpr;
        aggregate->SetInput(remainingFilter);
        deps->SetInput(aggregate);
        input = filter;

        return true;
    } else {
        return false;
    }

}

// Currently we only extract simple expressions where there is only one variable on either side

bool TExtractJoinExpressionsRule::MatchAndApply(std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    auto conjuncts = filter->FilterExpr.SplitConjunct();

    TVector<TExpression> matchedConjuncts;

    YQL_CLOG(TRACE, CoreDq) << "Testing expr extraction";

    for (auto & c : conjuncts) {
        if (c.MaybeJoinCondition(false)) {
            continue;
        }

        YQL_CLOG(TRACE, CoreDq) << "IUs in filter " << c.GetInputIUs().size();

        if (c.MaybeJoinCondition(true)) {
            matchedConjuncts.push_back(c);
        }
    }

    if (matchedConjuncts.size()) {
        TVector<TMapElement> mapElements;
        TNodeOnNodeOwnedMap replaceMap;

        for (auto & conj : matchedConjuncts) {
            TNodeOnNodeOwnedMap localMap;
            TVector<std::pair<TInfoUnit, TExprNode::TPtr>> renameMap;
            TJoinCondition cond(conj);
            cond.ExtractExpressions(localMap, renameMap);
            for (auto const & [key, value] : localMap) {
                replaceMap.insert({key, value});
            }
            for (auto const & [iu, expr] : renameMap) {
                mapElements.emplace_back(iu, TExpression(expr, &ctx.ExprCtx, &props));
            }
        }

        filter->FilterExpr.ApplyReplaceMap(replaceMap, ctx);
        auto newMap = std::make_shared<TOpMap>(filter->GetInput(), input->Pos, mapElements, false);
        filter->SetInput(newMap);
        return true;
    }

    return false;
}

// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

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

    // Check whether this is a correlated subplan with filter pushed up
    // FIXME: if the filter got stuck we will crash later in the optimizer
    if (subplan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto filter = CastOperator<TOpFilter>(subplan);
        auto addDeps = CastOperator<TOpAddDependencies>(filter->GetInput());
        auto uncorrSubplan = addDeps->GetInput();

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        TVector<TMapElement> mappings;
        mappings.push_back(TMapElement(subplanResIU, subplanResIU, filter->Pos, &ctx.ExprCtx, &props));

        auto leftIUs = child->GetOutputIUs();
        bool conflictsWithLeft = false;

        auto conjuncts = filter->FilterExpr.SplitConjunct();

        for (const auto & conj : conjuncts) {
            if (!conj.MaybeJoinCondition()) {
                continue;
            }

            TJoinCondition jc(conj);
            TInfoUnit leftKey = jc.GetLeftIU();
            TInfoUnit rightKey = jc.GetRightIU();

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), rightKey) != addDeps->Dependencies.end()) {
                std::swap(leftKey, rightKey);
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), leftKey) == addDeps->Dependencies.end()) {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }

            if (std::find(leftIUs.begin(), leftIUs.end(), rightKey) != leftIUs.end()) {
                auto newKey = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), false);
                mappings.push_back(TMapElement(newKey, rightKey, filter->Pos, &ctx.ExprCtx, &props));
                rightKey = newKey;
                conflictsWithLeft = true;
            } else {
                mappings.push_back(TMapElement(rightKey, rightKey, filter->Pos, &ctx.ExprCtx, &props));
            }

            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        if (conflictsWithLeft) {
            uncorrSubplan = std::make_shared<TOpMap>(uncorrSubplan, uncorrSubplan->Pos, mappings, true);
        }

        auto leftJoin = std::make_shared<TOpJoin>(child, uncorrSubplan, subplan->Pos, "Left", joinKeys);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, subplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = std::make_shared<TOpMap>(leftJoin, subplan->Pos, renameElements, false);
        unaryOp->SetInput(rename);
    }

    // Otherwise we assume an uncorrelated supbplan
    // Here we don't assume at most one tuple from the subplan
    else {
        auto emptySource = std::make_shared<TOpEmptySource>(subplan->Pos);

        TVector<TMapElement> mapElements;

        // FIXME: This works only for postgres types, because they are null-compatible
        // For YQL types we will need to handle optionality
        mapElements.emplace_back(scalarIU, MakeNothing(subplan->Pos, subplanResType, &ctx.ExprCtx));
        auto map = std::make_shared<TOpMap>(emptySource, subplan->Pos, mapElements, true);

        TVector<TMapElement> renameElements;
        renameElements.emplace_back(scalarIU, subplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        auto rename = std::make_shared<TOpMap>(subplan, subplan->Pos, renameElements, true);
        rename->Props.EnsureAtMostOne = true;

        auto unionAll = std::make_shared<TOpUnionAll>(rename, map, subplan->Pos, true);

        auto limit = std::make_shared<TOpLimit>(unionAll, subplan->Pos, MakeConstant("UInt64", "1", subplan->Pos, &ctx.ExprCtx));
    
        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        auto cross = std::make_shared<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}

std::shared_ptr<IOperator> TInlineSimpleInExistsSubplanRule::SimpleMatchAndApply(const std::shared_ptr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter || props.PgSyntax) {
        return input;
    }

    // Check that the filter lambda is a conjunction of one or more elements
    auto filter = CastOperator<TOpFilter>(input);
    auto lambdaBody = filter->FilterExpr.Node->ChildPtr(1);

    if (!TCoAnd::Match(lambdaBody.Get()) && !TCoNot::Match(lambdaBody.Get()) && !TCoMember::Match(lambdaBody.Get())) {
        return input;
    }

    // Decompose the conjunction into individual conjuncts
    auto conjuncts = filter->FilterExpr.SplitConjunct();

    // Find the first conjunct that is a simple in or exists subplan
    bool negated = false;
    TInfoUnit iu;
    TSubplanEntry subplanEntry;
    size_t conjunctIdx;

    for (conjunctIdx = 0; conjunctIdx < conjuncts.size(); conjunctIdx++) {
        auto maybeSubplan = conjuncts[conjunctIdx].Node;

        if (TCoNot::Match(maybeSubplan->ChildPtr(1).Get())) {
            maybeSubplan = maybeSubplan->ChildPtr(1)->ChildPtr(0);
            negated = true;
        }
        if (TCoMember::Match(maybeSubplan.Get())) {
            auto name = TString(maybeSubplan->ChildPtr(1)->Content());
            iu = TInfoUnit(name);
            if (props.Subplans.PlanMap.contains(iu)) {
                subplanEntry = props.Subplans.PlanMap.at(iu);
                if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || subplanEntry.Type == ESubplanType::EXISTS) {
                    break;
                }
            }
        }
    }

    if (conjunctIdx == conjuncts.size()) {
        return input;
    }

    std::shared_ptr<IOperator> join;
    TVector<std::pair<TInfoUnit, TInfoUnit>> extraJoinKeys;
    std::shared_ptr<IOperator> uncorrSubplan = subplanEntry.Plan;

    if (subplanEntry.Plan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplanEntry.Plan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto filter = CastOperator<TOpFilter>(subplanEntry.Plan);
        auto addDeps = CastOperator<TOpAddDependencies>(filter->GetInput());
        uncorrSubplan = addDeps->GetInput();

        for (const auto & conj : conjuncts) {
            if (!conj.MaybeJoinCondition()) {
                Y_ENSURE(false, "Expected a filter with only join conditions");
            }

            auto jc = TJoinCondition(conj);

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetLeftIU()) != addDeps->Dependencies.end()) {
                extraJoinKeys.push_back(std::make_pair(jc.GetLeftIU(), jc.GetRightIU()));
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), jc.GetRightIU()) != addDeps->Dependencies.end()) {
                extraJoinKeys.push_back(std::make_pair(jc.GetRightIU(), jc.GetLeftIU()));
            } else {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }   
        }
    }

    // We build a semi-join or a left-only join when processing IN subplan
    if (subplanEntry.Type == ESubplanType::IN_SUBPLAN || !extraJoinKeys.empty()) {
        auto leftJoinInput = filter->GetInput();
        auto joinKind = negated ? "LeftOnly" : "LeftSemi";

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        auto planIUs = uncorrSubplan->GetOutputIUs();

        for (size_t i = 0; i < subplanEntry.Tuple.size(); i++) {
            joinKeys.push_back(std::make_pair(subplanEntry.Tuple[i], planIUs[i]));
        }

        joinKeys.insert(joinKeys.begin(), extraJoinKeys.begin(), extraJoinKeys.end());

        join = std::make_shared<TOpJoin>(leftJoinInput, uncorrSubplan, input->Pos, joinKind, joinKeys);
        conjuncts.erase(conjuncts.begin() + conjunctIdx);
    }
    // EXISTS and NOT EXISTS
    else {
        auto limit = std::make_shared<TOpLimit>(uncorrSubplan, filter->Pos, MakeConstant("UInt64", "1", filter->Pos, &ctx.ExprCtx));

        auto countResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        TVector<TMapElement> countMapElements;
        auto zero = MakeConstant("UInt64", "0", filter->Pos, &ctx.ExprCtx);
        countMapElements.emplace_back(countResult, zero);
        auto countMap = std::make_shared<TOpMap>(limit, filter->Pos, countMapElements, true);

        TOpAggregationTraits aggFunction(countResult, "count", countResult);
        TVector<TOpAggregationTraits> aggs = {aggFunction};
        TVector<TInfoUnit> keyColumns;

        auto agg = std::make_shared<TOpAggregate>(countMap, aggs, keyColumns, EAggregationPhase::Final, false, filter->Pos);
        const TString compareCallable = negated ? "==" : "!=";

        auto comparePredicate = MakeBinaryPredicate(compareCallable, MakeColumnAccess(countResult, filter->Pos, &ctx.ExprCtx, &props), zero);
        TVector<TMapElement> mapElements;
        auto compareResult = TInfoUnit("_rbo_arg_" + std::to_string(props.InternalVarIdx++), true);
        mapElements.emplace_back(compareResult, comparePredicate);
        auto map = std::make_shared<TOpMap>(agg, filter->Pos, mapElements, true);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        join = std::make_shared<TOpJoin>(filter->GetInput(), map, filter->Pos, "Cross", joinKeys);

        conjuncts[conjunctIdx] = MakeColumnAccess(compareResult, filter->Pos, &ctx.ExprCtx, &props);
    }

    props.Subplans.Remove(iu);
    // If there was a single conjunct, we can get rid of the filter completely
    if (conjuncts.empty()) {
        return join;
    }

    // Otherwise, we need to pack the remaining conjuncts back into the filter
    auto remainingConjuncts = TVector<TExpression>(conjuncts.begin() + 1, conjuncts.end());
    return std::make_shared<TOpFilter>(join, filter->Pos, MakeConjunct(remainingConjuncts, props.PgSyntax));
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

    if (map->GetInput()->Kind != EOperator::Join) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(map->GetInput());
    bool canPushRight = join->JoinKind != "Left" && join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";
    bool canPushLeft = join->JoinKind != "Right" && join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";

    // Make sure the join and its inputs are single consumer
    // FIXME: join inputs don't have to be single consumer, but this used to break due to mutliple consumer problem
    if (!join->IsSingleConsumer() || !join->GetLeftInput()->IsSingleConsumer() || !join->GetRightInput()->IsSingleConsumer()) {
        return input;
    }

    TVector<TMapElement> leftMapElements;
    TVector<TMapElement> rightMapElements;
    TVector<TMapElement> topMapElements;
    TVector<int> removeElements;

    for (size_t i = 0; i < map->MapElements.size(); i++) {
        const auto & mapElement = map->MapElements[i];
        auto mapElIUs = mapElement.GetExpression().GetInputIUs(false, true);

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

std::shared_ptr<IOperator> TPushFilterUnderMapRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(input);
    if (filter->GetInput()->Kind != EOperator::Map) {
        return input;
    }

    auto map = CastOperator<TOpMap>(filter->GetInput());

    if (map->Project) {
        return input;
    }

    auto conjuncts = filter->FilterExpr.SplitConjunct();
    TVector<TExpression> pushedFilters;
    TVector<TExpression> remainingFilters;

    TVector<TInfoUnit> newMapColumns;
    for (const auto & mapEl : map->MapElements) {
        newMapColumns.push_back(mapEl.GetElementName());
    }

    for (const auto & c : conjuncts) {
        if (IUSetIntersect(c.GetInputIUs(false,true), newMapColumns).empty()){
            pushedFilters.push_back(c);
        } else {
            remainingFilters.push_back(c);
        }
    }

    if (pushedFilters.empty()) {
        return input;
    }

    filter->SetInput(map->GetInput());
    filter->FilterExpr = MakeConjunct(pushedFilters, props.PgSyntax);
    map->SetInput(filter);

    if (remainingFilters.size()) {
        auto pushedFilterExpr = MakeConjunct(remainingFilters, props.PgSyntax);
        return std::make_shared<TOpFilter>(map, map->Pos, pushedFilterExpr);
    } else {
        return map;
    }
}

// FIXME: We currently support pushing filter into Inner, Cross and Left Join
std::shared_ptr<IOperator> TPushFilterIntoJoinRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

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
        auto leftExpr = MakeConjunct(pushLeft, props.PgSyntax);
        leftInput = std::make_shared<TOpFilter>(leftInput, input->Pos, leftExpr);
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
                auto rightExpr = MakeConjunct(pushRight, props.PgSyntax);
                rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightExpr);
                join->JoinKind = "Inner";
            } else if (!pushLeft.size()) {
                return input;
            }
        } else {
            auto rightExpr = MakeConjunct(pushRight, props.PgSyntax);
            rightInput = std::make_shared<TOpFilter>(rightInput, input->Pos, rightExpr);
        }
    }

    join->SetLeftInput(leftInput);
    join->SetRightInput(rightInput);

    if (topLevelPreds.size()) {
        auto topFilterExpr = MakeConjunct(topLevelPreds, props.PgSyntax);
        output =  std::make_shared<TOpFilter>(join, input->Pos, topFilterExpr);
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
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
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
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
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

    auto newFilterExpr = TExpression(newLambda, filter->FilterExpr.Ctx, filter->FilterExpr.PlanProps);
    return std::make_shared<TOpFilter>(filter->GetInput(), input->Pos, newFilterExpr);
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
    const auto lambda = TCoLambda(filter->FilterExpr.Node);
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
            maybeFilter = FuseFilters(maybeFilter, maybeAnotherFilter, props.PgSyntax);
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