#include "kqp_plan_conversion_utils.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NNodes;

TExprNode::TPtr PlanConverter::RemoveSubplans(TExprNode::TPtr node) {
    auto lambda = TCoLambda(node);
    auto lambdaBody = lambda.Body().Ptr();

    auto sublinks = FindNodes(lambdaBody, [](const TExprNode::TPtr& n){ return TKqpSublinkBase::Match(n.Get()); });
    if (sublinks.empty()) {
        return node;
    }
    else {
        TNodeOnNodeOwnedMap replaceMap;

        for (auto link : sublinks) {
            auto sublinkVar = TInfoUnit("_rbo_arg_" + std::to_string(PlanProps.InternalVarIdx++), true);
            auto member = Build<TCoMember>(Ctx, lambda.Pos())
                    .Struct(lambda.Args().Arg(0).Ptr())
                    .Name<TCoAtom>().Value(sublinkVar.GetFullName()).Build()
                    .Done().Ptr();
            replaceMap[link.Get()] = member;
            auto subplan = ExprNodeToOperator(TKqpSublinkBase(link).Subquery().Ptr());
            TSubplanEntry entry;
            if (TKqpExprSublink::Match(link.Get())) {
                entry = TSubplanEntry(subplan, {}, ESubplanType::EXPR, sublinkVar);
            } else if (TKqpExistsSublink::Match(link.Get())) {
                entry = TSubplanEntry(subplan, {}, ESubplanType::EXISTS, sublinkVar);
            } else /* In sublink */ {
                auto tupleType = link->Child(TKqpInSublink::idx_InTuple);
                Y_ENSURE(tupleType->IsCallable("StructType"));
                TVector<TInfoUnit> tuple;

                //FIXME: Currently only a single element tuple in IN clause is supported, so we hardcode this case
                auto tupleElement = tupleType->Child(0)->Child(0)->Content();
                auto tupleElementIU = TInfoUnit(TString(tupleElement));
                entry = TSubplanEntry(subplan, {TInfoUnit(TString(tupleElement))}, ESubplanType::IN_SUBPLAN, sublinkVar);
            }
            PlanProps.Subplans.Add(sublinkVar, entry);
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

std::unique_ptr<TOpRoot> PlanConverter::ConvertRoot(TExprNode::TPtr node) {
    auto kqpOpRoot = TKqpOpRoot(node);
    auto rootInput = ExprNodeToOperator(kqpOpRoot.Input().Ptr());
    TVector<TString> columnOrder;

    for (const auto& column : kqpOpRoot.ColumnOrder()) {
        columnOrder.push_back(column.StringValue());
    }

    auto opRoot = std::make_unique<TOpRoot>(rootInput, node->Pos(), columnOrder);
    opRoot->Node = node;
    opRoot->PlanProps = PlanProps;
    opRoot->PlanProps.PgSyntax = std::stoi(kqpOpRoot.PgSyntax().StringValue());
 
    // We need to propagate plan properties reference into expressions in the plan
    for (auto it : *opRoot) {
        for (auto exprRef : it.Current->GetExpressions()) {
            exprRef.get().PlanProps = &(opRoot->PlanProps);
        }
    }

   return opRoot;
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
    } else if (NYql::NNodes::TKqpInfuseDependents::Match(node.Get())) {
        result = ConvertTKqpInfuseDependents(node);
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

bool GetForceOptional(const TKqpOpMapElementLambda& mapElement) {
    auto maybeForceOptional = mapElement.ForceOptional();
    return maybeForceOptional && maybeForceOptional.Cast().StringValue() == "True";
}

bool GetOrdered(const TKqpOpMap& map) {
    auto maybeOrdered = map.Ordered();
    return maybeOrdered && maybeOrdered.Cast().StringValue() == "True";
}

TExprNode::TPtr GetMapElementLambda(TExprNode::TPtr lambdaPtr, const bool forceOptional, TExprContext& ctx) {
    auto lambda = TCoLambda(lambdaPtr);
    auto body = lambda.Body().Ptr();
    auto lambdaArg = lambda.Args().Arg(0);
    auto bodyType = body->GetTypeAnn();
    Y_ENSURE(bodyType);
    // Force optional by adding Just.
    if (!bodyType->IsOptionalOrNull() && forceOptional) {
        // clang-format off
        body = Build<TCoJust>(ctx, lambdaPtr->Pos())
            .Input(body)
        .Done().Ptr();

        lambdaPtr = Build<TCoLambda>(ctx, lambdaPtr->Pos())
            .Args({"arg"})
            .Body<TExprApplier>()
                .Apply(TExprBase(body))
                .With(lambdaArg, "arg")
            .Build()
        .Done().Ptr();
        // clang-format on
    }
    return lambdaPtr;
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpMap(TExprNode::TPtr node) {
    auto opMap = TKqpOpMap(node);
    auto input = ExprNodeToOperator(opMap.Input().Ptr());
    auto project = opMap.Project().IsValid();
    const auto ordered = GetOrdered(opMap);
    TVector<TMapElement> mapElements;

    for (const auto& mapElement : opMap.MapElements()) {
        const auto iu = TInfoUnit(mapElement.Variable().StringValue());
        if (mapElement.Maybe<TKqpOpMapElementRename>()) {
            auto element = mapElement.Cast<TKqpOpMapElementRename>();
            auto fromIU = TInfoUnit(element.From().StringValue());
            mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx);
        } else {
            auto element = mapElement.Cast<TKqpOpMapElementLambda>();
            const auto forceOptional = GetForceOptional(element);
            // case lambda ($arg) { member $arg `name }
            if (auto maybeMember = element.Lambda().Body().Maybe<TCoMember>();
                !forceOptional && maybeMember && maybeMember.Cast().Struct().Ptr() == element.Lambda().Args().Arg(0).Ptr()) {
                auto member = maybeMember.Cast();
                auto name = member.Name().Cast<TCoAtom>();
                auto fromIU = TInfoUnit(name.StringValue());
                mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx);
            } else {
                TExpression exprLambda(GetMapElementLambda(element.Lambda().Ptr(), forceOptional, Ctx), &Ctx);
                mapElements.emplace_back(iu, exprLambda);
            }
        }
    }
    return std::make_shared<TOpMap>(input, node->Pos(), mapElements, project, ordered);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpInfuseDependents(TExprNode::TPtr node) {
    auto opInfuseDeps = TKqpInfuseDependents(node);
    auto input = ExprNodeToOperator(opInfuseDeps.Input().Ptr());
    TVector<TInfoUnit> columns;
    TVector<const TTypeAnnotationNode*> types;

    for (auto c : opInfuseDeps.Columns()) {
        columns.push_back(TInfoUnit(c.StringValue()));
    }

    for (auto typeExpr : opInfuseDeps.Types()) {
        auto type = typeExpr.Ptr()->GetTypeAnn();
        types.push_back(type->Cast<TTypeExprType>()->GetType());
    }

    return std::make_shared<TOpAddDependencies>(input, node->Pos(), columns, types);
}


std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpFilter(TExprNode::TPtr node) {
    auto opFilter = TKqpOpFilter(node);
    auto input = ExprNodeToOperator(opFilter.Input().Ptr());
    auto lambda = opFilter.Lambda().Ptr();
    auto newLambda = RemoveSubplans(lambda);
    return std::make_shared<TOpFilter>(input, node->Pos(), TExpression(newLambda, &Ctx));
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
    TExpression count(opLimit.Count().Ptr(), &Ctx);
    return std::make_shared<TOpLimit>(input, node->Pos(), count);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpProject(TExprNode::TPtr node) {
    auto opProject = TKqpOpProject(node);
    auto input = ExprNodeToOperator(opProject.Input().Ptr());

    TVector<TInfoUnit> projectList;
    for (const auto& p : opProject.ProjectList()) {
        projectList.push_back(TInfoUnit(p.StringValue()));
    }
    return std::make_shared<TOpProject>(input, node->Pos(), projectList);
}

std::shared_ptr<IOperator> PlanConverter::ConvertTKqpOpSort(TExprNode::TPtr node) {
    auto opSort = TKqpOpSort(node);
    auto input = ExprNodeToOperator(opSort.Input().Ptr());
    auto output = input;

    TVector<TSortElement> sortElements;
    TVector<TMapElement> mapElements;

    for (const auto& el : opSort.SortExpressions()) {
        TInfoUnit column;

        if (auto member = el.Lambda().Body().Maybe<TCoMember>()) {
            column = TInfoUnit(member.Cast().Name().StringValue());
        } else {
            TString newName = "_rbo_arg_" + std::to_string(PlanProps.InternalVarIdx++);
            column = TInfoUnit(newName);
            mapElements.emplace_back(column, TExpression(el.Lambda().Ptr(), &Ctx));
        }
        sortElements.push_back(TSortElement(column, el.Direction().StringValue() == "asc", el.NullsFirst().StringValue() == "first"));
    }

    if (mapElements.size()) {
        output = std::make_shared<TOpMap>(input, input->Pos, mapElements, false);
    }

    output = std::make_shared<TOpSort>(output, node->Pos(), sortElements);
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
    for (const auto& keyColumn : opAggregate.KeyColumns()) {
        keyColumns.push_back(TInfoUnit(TString(keyColumn)));
    }

    const bool distinctAll = opAggregate.DistinctAll() == "True" ? true : false;
    return std::make_shared<TOpAggregate>(input, opAggTraitsList, keyColumns, EAggregationPhase::Final, distinctAll, node->Pos());
}

} // namespace NKqp
} // namespace NKikimr