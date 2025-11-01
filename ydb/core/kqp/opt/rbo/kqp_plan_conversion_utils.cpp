#include "kqp_plan_conversion_utils.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

TExprNode::TPtr PlanConverter::RemoveScalarSubplans(TExprNode::TPtr node) {
    auto lambda = TCoLambda(node);
    auto lambdaBody = lambda.Body().Ptr();

    auto exprSublinks = FindNodes(lambdaBody, [](const TExprNode::TPtr& n){return n->IsCallable("KqpPgExprSublink");});
    if (exprSublinks.empty()) {
        return node;
    }
    else {
        TNodeOnNodeOwnedMap replaceMap;

        for (auto link : exprSublinks) {
            auto sublinkVar = TInfoUnit("_rbo_arg_" + std::to_string(PlanProps.InternalVarIdx++), true);
            auto member = Build<TCoMember>(Ctx, lambda.Pos())
                    .Struct(lambda.Args().Arg(0).Ptr())
                    .Name<TCoAtom>().Value(sublinkVar.GetFullName()).Build()
                    .Done().Ptr();
            replaceMap[link.Get()] = member;
            auto subplan = ExprNodeToOperator(TKqpPgExprSublink(link).Expr().Ptr());
            PlanProps.ScalarSubplans.Add(sublinkVar, subplan);
        }

        TOptimizeExprSettings settings(&TypeCtx);
        TExprNode::TPtr newLambdaBody;
        RemapExpr(lambdaBody, newLambdaBody, replaceMap, Ctx, settings);

        return Build<TCoLambda>(Ctx, lambda.Pos())
            .Args(lambda.Args())
            .Body(newLambdaBody)
            .Done().Ptr();
    }
}

TOpRoot PlanConverter::ConvertRoot(TExprNode::TPtr node) {
    auto opRoot = TKqpOpRoot(node);
    auto rootInput = ExprNodeToOperator(opRoot.Input().Ptr());
    auto res = TOpRoot(rootInput, node->Pos());
    res.Node = node;
    res.PlanProps = PlanProps;
    return res;
}

std::shared_ptr<IOperator> PlanConverter::ExprNodeToOperator(TExprNode::TPtr node) {
    if (Converted.contains(node.Get())) {
        return Converted.at(node.Get());
    }

    std::shared_ptr<IOperator> result;
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        result = std::make_shared<TOpEmptySource>(node->Pos());
    } else if (NYql::NNodes::TKqpOpRead::Match(node.Get())) {
        result = std::make_shared<TOpRead>(node);
    } else if (NYql::NNodes::TKqpOpMap::Match(node.Get())) {
        result = ConvertTKqpOpMap(node);
    } else if (NYql::NNodes::TKqpOpFilter::Match(node.Get())) {
        result = ConvertTKqpOpFilter(node);
    } else if (NYql::NNodes::TKqpOpJoin::Match(node.Get())) {
        result = ConvertTKqpOpJoin(node);
    } else if (NYql::NNodes::TKqpOpLimit::Match(node.Get())) {
        result = ConvertTKqpOpLimit(node);
    } else if (NYql::NNodes::TKqpOpProject::Match(node.Get())) {
        result = ConvertTKqpOpProject(node);
    } else if (NYql::NNodes::TKqpOpUnionAll::Match(node.Get())) {
        result = ConvertTKqpOpUnionAll(node);
    } else if (NYql::NNodes::TKqpOpSort::Match(node.Get())) {
        result = ConvertTKqpOpSort(node);
    } else if (NYql::NNodes::TKqpOpAggregate::Match(node.Get())) {
        result = ConvertTKqpOpAggregate(node);
    } else {
        YQL_ENSURE(false, "Unknown operator node");
    }
    Converted[node.Get()] = result;
    return result;
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpMap(TExprNode::TPtr node) {
    auto opMap = TKqpOpMap(node);
    auto input = ExprNodeToOperator(opMap.Input().Ptr());
    auto project = opMap.Project().IsValid();
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements;

    for (auto mapElement : opMap.MapElements()) {
        auto iu = TInfoUnit(mapElement.Variable().StringValue());
        if (mapElement.Maybe<TKqpOpMapElementRename>()) {
            auto element = mapElement.Cast<TKqpOpMapElementRename>();
            auto fromIU = TInfoUnit(element.From().StringValue());
            mapElements.push_back(std::make_pair(iu, fromIU));
        } else {
            auto element = mapElement.Cast<TKqpOpMapElementLambda>();
            if (element.Lambda().Body().Maybe<TCoMember>().Name().Maybe<TCoAtom>()) {
                auto member = element.Lambda().Body().Cast<TCoMember>();
                auto name = member.Name().Cast<TCoAtom>();
                auto fromIU = TInfoUnit(name.StringValue());
                mapElements.push_back(std::make_pair(iu, fromIU));
            } else {
                mapElements.push_back(std::make_pair(iu, element.Lambda().Ptr()));
            }
        }
    }
    return std::make_shared<TOpMap>(input, node->Pos(), mapElements, project);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpFilter(TExprNode::TPtr node) {
    auto opFilter = TKqpOpFilter(node);
    auto input = ExprNodeToOperator(opFilter.Input().Ptr());
    auto lambda = opFilter.Lambda().Ptr();
    auto newLambda = RemoveScalarSubplans(lambda);
    return std::make_shared<TOpFilter>(input, node->Pos(), newLambda);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpJoin(TExprNode::TPtr node) {
    auto opJoin = TKqpOpJoin(node);

    auto leftInput = ExprNodeToOperator(opJoin.LeftInput().Ptr());
    auto rightInput = ExprNodeToOperator(opJoin.RightInput().Ptr());

    auto joinKind = opJoin.JoinKind().StringValue();
    TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
    for (auto k : opJoin.JoinKeys()) {
        TInfoUnit leftKey(k.LeftLabel().StringValue(), k.LeftColumn().StringValue());
        TInfoUnit rightKey(k.RightLabel().StringValue(), k.RightColumn().StringValue());

        joinKeys.push_back(std::make_pair(leftKey, rightKey));
    }
    return std::make_shared<TOpJoin>(leftInput, rightInput, node->Pos(), joinKind, joinKeys);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpUnionAll(TExprNode::TPtr node) {
    auto opUnionAll = TKqpOpUnionAll(node);
    auto leftInput = ExprNodeToOperator(opUnionAll.LeftInput().Ptr());
    auto rightInput = ExprNodeToOperator(opUnionAll.RightInput().Ptr());

    return std::make_shared<TOpUnionAll>(leftInput, rightInput, node->Pos());
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpLimit(TExprNode::TPtr node) {
    auto opLimit = TKqpOpLimit(node);
    auto input = ExprNodeToOperator(opLimit.Input().Ptr());
    return std::make_shared<TOpLimit>(input, node->Pos(), opLimit.Count().Ptr());
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpProject(TExprNode::TPtr node) {
    auto opProject = TKqpOpProject(node);
    auto input = ExprNodeToOperator(opProject.Input().Ptr());

    TVector<TInfoUnit> projectList;

    for (auto p : opProject.ProjectList()) {
        projectList.push_back(TInfoUnit(p.StringValue()));
    }
    return std::make_shared<TOpProject>(input, node->Pos(), projectList);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpSort(TExprNode::TPtr node) {
    auto opSort = TKqpOpSort(node);
    auto input = ExprNodeToOperator(opSort.Input().Ptr());
    auto output = input;

    TVector<TSortElement> sortElements;
    TVector<std::pair<TInfoUnit, std::variant<TInfoUnit, TExprNode::TPtr>>> mapElements;

    for (auto el : opSort.SortExpressions()) {
        TInfoUnit column;

        if (auto member = el.Lambda().Body().Maybe<TCoMember>()) {
            column = TInfoUnit(member.Cast().Name().StringValue());
        } else {
            TString newName = "_rbo_arg_" + std::to_string(PlanProps.InternalVarIdx++);
            column = TInfoUnit(newName);
            mapElements.push_back(std::make_pair(column, el.Lambda().Ptr()));
        }
        sortElements.push_back(TSortElement(column, el.Direction().StringValue() == "asc", el.NullsFirst().StringValue() == "first"));
    }

    if (mapElements.size()) {
        output = std::make_shared<TOpMap>(input, input->Pos, mapElements, false);
    }

    output->Props.OrderEnforcer = TOrderEnforcer(EOrderEnforcerAction::REQUIRE, EOrderEnforcerReason::USER, sortElements);
    return output;
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpAggregate(TExprNode::TPtr node) {
    auto opAggregate = TKqpOpAggregate(node);
    auto input = ExprNodeToOperator(opAggregate.Input().Ptr());

    TVector<TOpAggregationTraits> opAggTraitsList;
    for (const auto& traits : opAggregate.AggregationTraitsList()) {
        const auto originalColName = TInfoUnit(TString(traits.OriginalColName()));
        const auto aggFuncName = TString(traits.AggregationFunction());
        const auto resultColName = TInfoUnit(TString(traits.ResultColName()));
        TOpAggregationTraits opAggTraits(originalColName, aggFuncName, resultColName);
        opAggTraitsList.push_back(opAggTraits);
    }

    TVector<TInfoUnit> keyColumns;
    for (const auto &keyColumn : opAggregate.KeyColumns()) {
        keyColumns.push_back(TInfoUnit(TString(keyColumn)));
    }

    return std::make_shared<TOpAggregate>(input, opAggTraitsList, keyColumns, EAggregationPhase::Final, node->Pos());
}

} // namespace NKqp
} // namespace NKikimr