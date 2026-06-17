#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_utils.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NNodes;

using DependencyPairType = std::pair<TInfoUnit, const TTypeAnnotationNode*>;

std::optional<TInfoUnit> ResolveVisibleIUByColumnName(const TVector<TInfoUnit>& visibleIUs, const TInfoUnit& iu) {
    if (ContainsInfoUnit(visibleIUs, iu)) {
        return iu;
    }

    std::optional<TInfoUnit> candidate;
    for (const auto& visible : visibleIUs) {
        if (visible.GetColumnName() != iu.GetColumnName()) {
            continue;
        }
        if (candidate) {
            return std::nullopt;
        }
        candidate = visible;
    }
    return candidate;
}

bool OutputsBothJoinSides(const TString& joinKind) {
    return joinKind != "LeftOnly" && joinKind != "LeftSemi" && joinKind != "RightOnly" && joinKind != "RightSemi";
}

void RenameRightGeneratedIgnoreConflicts(
    const TIntrusivePtr<IOperator>& leftInput,
    TIntrusivePtr<IOperator>& rightInput,
    TPositionHandle pos,
    TExprContext& ctx,
    TPlanProps& props)
{
    const auto leftOutput = MakeInfoUnitSet(leftInput->GetOutputIUs());
    TVector<TMapElement> mapElements;

    for (const auto& rightIU : rightInput->GetOutputIUs()) {
        if (!IsGeneratedIgnoreIU(rightIU) || !leftOutput.contains(rightIU)) {
            continue;
        }

        mapElements.emplace_back(MakeGeneratedIgnoreIU(props), rightIU, pos, &ctx, &props);
    }

    if (mapElements.empty()) {
        return;
    }

    rightInput = MakeIntrusive<TOpMap>(rightInput, pos, mapElements);
}

/**
 * Computes dependent variables and updates the plan
 */
TVector<DependencyPairType> ComputeDependentVariables(TIntrusivePtr<IOperator> op, TPlanProps* props) {

    TVector<DependencyPairType> subplanDependencies;

    // Iterate over just the operator of the current plan/subplan
    auto it = TOpIterator(op, nullptr);
    for(; it != TOpIterator(nullptr); it++) {
        auto currOp = (*it).Current;
        auto subplanIUs = currOp->GetSubplanIUs(*props);

        // If the current operator contains references to subplans:
        // - Compute dependent variables of the subplan
        // - Update the subplan list of dependent variables
        // - Filter out variables that have into units that don't match current ius (these are inner dependencies)
        // - Add new dependencies to the AddDepencies operator below the current, or create one if it doesn't exit
        // - Return the full list of dependecies

        if (subplanIUs.size()) {
            auto unaryOp = CastOperator<IUnaryOperator>(currOp);

            TVector<DependencyPairType> allOpDependencies;

            for (const auto & subplanVar : subplanIUs) {
                auto & subplanEntry = props->Subplans.PlanMap.at(subplanVar);
                auto opDependencies = ComputeDependentVariables(CastOperator<IOperator>(subplanEntry.Plan), props);
                if (opDependencies.size()) {
                    for (const auto & [iu, type] : opDependencies) {
                        subplanEntry.DependentIUs.push_back(iu);
                    }
                    AddUnique<DependencyPairType>(opDependencies, allOpDependencies);
                }
            }

            auto outputIUs = unaryOp->GetInput()->GetOutputIUs();
            TVector<DependencyPairType> filteredOpDependencies;
            for (const auto & d : allOpDependencies) {
                if (std::find(outputIUs.begin(), outputIUs.end(), d.first) == outputIUs.end()) {
                    filteredOpDependencies.push_back(d);
                }
            }

            if (filteredOpDependencies.size()) {
                if (unaryOp->GetInput()->Kind != EOperator::AddDependencies) {
                    auto addDeps = MakeIntrusive<TOpAddDependencies>(unaryOp->GetInput(), unaryOp->Pos, filteredOpDependencies);
                    unaryOp->SetInput(addDeps);
                } else {
                    auto addDeps = CastOperator<TOpAddDependencies>(unaryOp->GetInput());
                    auto depPairs = addDeps->GetDependencyPairs();
                    AddUnique<DependencyPairType>(filteredOpDependencies, depPairs);
                    addDeps->SetDependencyPairs(depPairs);
                }
            }

            AddUnique<DependencyPairType>(filteredOpDependencies, subplanDependencies);
        }

        if (currOp->Kind == EOperator::AddDependencies) {
            auto addDeps = CastOperator<TOpAddDependencies>(currOp);
            auto depPairs = addDeps->GetDependencyPairs();
            AddUnique<DependencyPairType>(depPairs, subplanDependencies);
        }
    }

    return subplanDependencies;
}

bool GetForceOptional(const TKqpOpMapElementLambda& mapElement) {
    return mapElement.ForceOptional().StringValue() == "True";
}

bool GetOrdered(const TKqpOpMap& map) {
    auto maybeOrdered = map.Ordered();
    return maybeOrdered && maybeOrdered.Cast().StringValue() == "True";
}

bool GetProject(const TKqpOpMap& map) {
    return map.Project().IsValid();
}

bool GetDistinct(const TKqpOpAggregationTraits& aggTraits) {
    auto maybeDistinct = aggTraits.Distinct();
    return maybeDistinct && maybeDistinct.Cast().StringValue() == "distinct";
}

TVector<TInfoUnit> GetStructIUs(const TTypeAnnotationNode* type) {
    Y_ENSURE(type);
    const auto* structType = type->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TVector<TInfoUnit> result;
    result.reserve(structType->GetItems().size());
    for (const auto& item : structType->GetItems()) {
        result.emplace_back(TString(item->GetName()));
    }
    return result;
}

} // anonymous namespace

TExprNode::TPtr PlanConverter::RemoveSubplans(TExprNode::TPtr node) {
    auto lambda = TCoLambda(node);
    auto lambdaBody = lambda.Body().Ptr();

    auto sublink = FindNode(lambdaBody, [](const TExprNode::TPtr& n){ return TKqpSublinkBase::Match(n.Get()); });
    if (!sublink) {
        return node;
    } else {
        TExprNode::TPtr newLambdaBody = lambdaBody;

        while(sublink){
            TNodeOnNodeOwnedMap replaceMap;

            YQL_CLOG(TRACE, CoreDq) << "Replacing sublink: " << PrintRBOExpression(sublink, Ctx);
            auto sublinkVar = TInfoUnit("_rbo_arg_" + std::to_string(PlanProps.InternalVarIdx++), true);
            // clang-format off
            auto member = Build<TCoMember>(Ctx, lambda.Pos())
                    .Struct(lambda.Args().Arg(0).Ptr())
                    .Name<TCoAtom>().Value(sublinkVar.GetFullName()).Build()
                    .Done().Ptr();
            replaceMap[sublink.Get()] = member;
            // clang-format on
            auto subplan = ExprNodeToOperator(TKqpSublinkBase(sublink).Subquery().Ptr());
            TSubplanEntry entry;
            if (TKqpExprSublink::Match(sublink.Get())) {
                entry = TSubplanEntry(subplan, {}, ESubplanType::EXPR, sublinkVar);
            } else if (TKqpExistsSublink::Match(sublink.Get())) {
                entry = TSubplanEntry(subplan, {}, ESubplanType::EXISTS, sublinkVar);
            } else /* In sublink */ {
                auto lambda = sublink->Child(TKqpInSublink::idx_InLambda);

                Y_ENSURE(lambda->IsLambda());
                TVector<TInfoUnit> tuple;

                auto lambdaBody = lambda->Child(1);
                //FIXME: Only YQL syntax is supported in this case, as we'll need to process the postgresql callable for equality
                Y_ENSURE(lambdaBody->IsCallable("=="));
                auto lhs = lambdaBody->Child(0);

                // FIXME: current we only support a single member in IN clause
                Y_ENSURE(lhs->IsCallable("Member"), "Only a single column reference in the IN clause is supported");
                
                if (lhs->IsCallable("Member")) {
                    auto iu = TInfoUnit(TString(lhs->Child(1)->Content()));
                    YQL_CLOG(TRACE, CoreDq) << "Processing: " << iu.GetFullName();

                    tuple.push_back(iu);
                } 

                // else if (lhs->IsList()) {
                //  for (const auto & member : lhs->Children()) {
                //    Y_ENSURE(member->IsCallable("Member"));
                //    tuple.push_back(TInfoUnit(TString(member->Child(1)->Content())));
                //}
                //} else {
                //    Y_ENSURE(false, "Unsupported callable in IN sublink");
                //}

                entry = TSubplanEntry(subplan, tuple, ESubplanType::IN_SUBPLAN, sublinkVar);
            }
            PlanProps.Subplans.Add(sublinkVar, entry);
            TOptimizeExprSettings settings(&TypeCtx);
            RemapExpr(newLambdaBody, newLambdaBody, replaceMap, Ctx, settings);

            sublink = FindNode(newLambdaBody, [](const TExprNode::TPtr& n){ return TKqpSublinkBase::Match(n.Get()); });
        }

        // clang-format off
        return Build<TCoLambda>(Ctx, lambda.Pos())
            .Args(lambda.Args())
            .Body(newLambdaBody)
            .Done().Ptr();
        // clang-format on
    }
}

TIntrusivePtr<TOpRoot> PlanConverter::ConvertRoot(TExprNode::TPtr node) {
    auto kqpOpRoot = TKqpOpRoot(node);
    auto rootInput = ExprNodeToOperator(kqpOpRoot.Input().Ptr());
    TVector<TString> columnOrder;

    for (const auto& column : kqpOpRoot.ColumnOrder()) {
        columnOrder.push_back(column.StringValue());
    }

    auto opRoot = MakeIntrusive<TOpRoot>(rootInput, node->Pos(), columnOrder);
    opRoot->Node = node;
    opRoot->PlanProps = PlanProps;
 
    // We need to propagate plan properties reference into expressions in the plan
    for (auto it : *opRoot) {
        for (auto exprRef : it.Current->GetExpressions()) {
            exprRef.get().PlanProps = &(opRoot->PlanProps);
        }
    }

    // For subplans, we need to compute dependent variables correctly
    ComputeDependentVariables(opRoot, &opRoot->PlanProps);

   return opRoot;
}

TIntrusivePtr<IOperator> PlanConverter::ExprNodeToOperator(TExprNode::TPtr node) {
    if (Converted.contains(node.Get())) {
        return Converted.at(node.Get());
    }

    TIntrusivePtr<IOperator> result;
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        result = MakeIntrusive<TOpEmptySource>(node->Pos());
    } else if (NYql::NNodes::TKqpOpRead::Match(node.Get())) {
        result = MakeIntrusive<TOpRead>(node);
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
    } else if (NYql::NNodes::TKqpOpSetOp::Match(node.Get())) {
        result = ConvertTKqpOpSetOp(node);
    } else if (NYql::NNodes::TKqpOpSort::Match(node.Get())) {
        result = ConvertTKqpOpSort(node);
    } else if (NYql::NNodes::TKqpOpAggregate::Match(node.Get())) {
        result = ConvertTKqpOpAggregate(node);
    } else if (NYql::NNodes::TKqpOpReplaceAlias::Match(node.Get())) {
        result = ConvertTKqpOpReplaceAlias(node);
    } else {
        YQL_ENSURE(false, "Unknown operator node");
    }
    Converted[node.Get()] = result;
    return result;
}

TExprNode::TPtr GetMapElementLambda(TExprNode::TPtr lambdaPtr, const bool forceOptional, TExprContext& ctx) {
    auto lambda = TCoLambda(lambdaPtr);
    auto body = lambda.Body().Ptr();
    auto lambdaArg = lambda.Args().Arg(0);
    const TTypeAnnotationNode* bodyType = body->GetTypeAnn();
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

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpMap(TExprNode::TPtr node) {
    auto opMap = TKqpOpMap(node);
    auto input = ExprNodeToOperator(opMap.Input().Ptr());
    const auto project = GetProject(opMap);
    const auto ordered = GetOrdered(opMap);
    TVector<TMapElement> mapElements;

    for (const auto& mapElement : opMap.MapElements()) {
        const auto iu = TInfoUnit(mapElement.Variable().StringValue());
        if (mapElement.Maybe<TKqpOpMapElementRename>()) {
            auto element = mapElement.Cast<TKqpOpMapElementRename>();
            auto fromIU = TInfoUnit(element.From().StringValue());
            mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx, &PlanProps, project);
        } else {
            auto element = mapElement.Cast<TKqpOpMapElementLambda>();
            const auto forceOptional = GetForceOptional(element);
            // case lambda ($arg) { member $arg `name }
            if (auto maybeMember = element.Lambda().Body().Maybe<TCoMember>();
                !forceOptional && maybeMember && maybeMember.Cast().Struct().Ptr() == element.Lambda().Args().Arg(0).Ptr()) {
                auto member = maybeMember.Cast();
                auto name = member.Name().Cast<TCoAtom>();
                auto fromIU = TInfoUnit(name.StringValue());
                mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx, &PlanProps, project);
            } else {
                TExpression exprLambda(GetMapElementLambda(element.Lambda().Ptr(), forceOptional, Ctx), &Ctx);
                mapElements.emplace_back(iu, exprLambda);
            }
        }
    }

    if (project) {
        TInfoUnitSet renameSources;
        for (const auto& mapElement : mapElements) {
            if (mapElement.IsRename()) {
                renameSources.insert(mapElement.GetRename());
            }
        }
        for (const auto& iu : input->GetOutputIUs()) {
            if (renameSources.contains(iu) || IsGeneratedIgnoreIU(iu)) {
                continue;
            }
            mapElements.emplace_back(MakeGeneratedIgnoreIU(PlanProps), iu, node->Pos(), &Ctx, &PlanProps);
        }
    }

    return MakeIntrusive<TOpMap>(input, node->Pos(), mapElements, ordered);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpInfuseDependents(TExprNode::TPtr node) {
    auto opInfuseDeps = TKqpInfuseDependents(node);
    auto input = ExprNodeToOperator(opInfuseDeps.Input().Ptr());
    TVector<TInfoUnit> columns;
    TVector<const TTypeAnnotationNode*> types;

    for (auto c : opInfuseDeps.Columns()) {
        columns.push_back(TInfoUnit(c.StringValue()));
    }

    for (auto typeExpr : opInfuseDeps.Types()) {
        const TTypeAnnotationNode* type = typeExpr.Ptr()->GetTypeAnn();
        types.push_back(type->Cast<TTypeExprType>()->GetType());
    }

    return MakeIntrusive<TOpAddDependencies>(input, node->Pos(), columns, types);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpFilter(TExprNode::TPtr node) {
    auto opFilter = TKqpOpFilter(node);
    auto input = ExprNodeToOperator(opFilter.Input().Ptr());
    auto lambda = opFilter.Lambda().Ptr();
    auto newLambda = RemoveSubplans(lambda);
    auto filter = MakeIntrusive<TOpFilter>(input, node->Pos(), TExpression(newLambda, &Ctx));
    YQL_CLOG(TRACE, CoreDq) << "Processed filter, new lambda " << filter->ToString(Ctx);
    return filter;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpJoin(TExprNode::TPtr node) {
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

    TVector<TExpression> joinFilters;
    for (auto f : opJoin.JoinFilters()) {
        joinFilters.push_back(TExpression(f.Lambda().Ptr(), &Ctx));
    }

    if (OutputsBothJoinSides(joinKind)) {
        RenameRightGeneratedIgnoreConflicts(leftInput, rightInput, node->Pos(), Ctx, PlanProps);
    }

    return MakeIntrusive<TOpJoin>(leftInput, rightInput, node->Pos(), joinKind, joinKeys, joinFilters);
}

TExprNode::TPtr MaybeForceColumnToOptional(const TTypeAnnotationNode* unionAllType, TExprNode::TPtr input, TExprContext& ctx) {
    Y_ENSURE(unionAllType);
    auto inputType = input->GetTypeAnn();
    Y_ENSURE(inputType);

    Y_ENSURE(TMaybeNode<TKqpOpMap>(input), "Input is not a KqpOpMap.");
    auto unionAllStructType = unionAllType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto inputStructType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    const auto unionAllSize = unionAllStructType->GetItems().size();
    Y_ENSURE(unionAllSize == inputStructType->GetItems().size());
    auto map = TExprBase(input).Cast<TKqpOpMap>();
    Y_ENSURE(unionAllSize == map.MapElements().Size(), "Invalid number of input fields.");

    TVector<TExprNode> mapElements;
    for (ui32 i = 0; i < unionAllSize; ++i) {
        const auto mapElement = map.MapElements().Item(i).Ptr();
        const TString fieldName = TString(mapElement->ChildPtr(1)->Content());
        auto inputFieldType = inputStructType->FindItemType(fieldName);
        Y_ENSURE(inputFieldType, TStringBuilder() << "Cannot find type for item " << fieldName;);
        auto unionAllFieldType = unionAllStructType->FindItemType(fieldName);
        Y_ENSURE(unionAllFieldType, TStringBuilder() << "Cannot find type for item " << fieldName;);
        // In case union all field type is optional but the same field for input is not - force optional.
        if (unionAllFieldType->IsOptionalOrNull() && !inputFieldType->IsOptionalOrNull()) {
            mapElement->ChildRef(3) = Build<TCoAtom>(ctx, input->Pos()).Value("True").Done().Ptr();
        }
    }

    return input;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpSetOp(TExprNode::TPtr node) {
    auto opSetOp = TKqpOpSetOp(node);

    auto leftInputPtr = opSetOp.LeftInput().Ptr();
    auto rightInputPtr = opSetOp.RightInput().Ptr();
    if (TMaybeNode<TKqpOpMap>(leftInputPtr)) {
        leftInputPtr = MaybeForceColumnToOptional(node->GetTypeAnn(), leftInputPtr, Ctx);
    }
    if (TMaybeNode<TKqpOpMap>(rightInputPtr)) {
        rightInputPtr = MaybeForceColumnToOptional(node->GetTypeAnn(), rightInputPtr, Ctx) ;
    }

    auto leftInput = ExprNodeToOperator(leftInputPtr);
    auto rightInput = ExprNodeToOperator(rightInputPtr);

    TString setOpKind = opSetOp.SetOp().StringValue();
    TIntrusivePtr<IOperator> result;

    if (setOpKind == "intersect_all" || setOpKind == "except_all") {
        Y_ENSURE(false, TStringBuilder() << "Set operation " << setOpKind << " is not currently supported");
    }

    if (setOpKind == "union_all" || setOpKind == "union") {
        const auto outputIUs = GetStructIUs(node->GetTypeAnn());

        const auto lhsSourceIUs = GetStructIUs(leftInputPtr->GetTypeAnn());
        const auto rhsSourceIUs = GetStructIUs(rightInputPtr->GetTypeAnn());
        Y_ENSURE(outputIUs.size() == lhsSourceIUs.size(), "UnionAll output and left input column counts mismatch");
        Y_ENSURE(outputIUs.size() == rhsSourceIUs.size(), "UnionAll output and right input column counts mismatch");

        result = MakeIntrusive<TOpUnionAll>(leftInput, rightInput, node->Pos(), outputIUs);
    } else if (setOpKind == "intersect" || setOpKind == "except" ) {

        auto itemType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        TVector<TInfoUnit> setOpColumns;
        for (const auto& t : itemType->GetItems()) {
            if (t->GetItemType()->IsOptionalOrNull()) {
                Y_ENSURE(false, TStringBuilder() << "Intersect/except key columns cannot be nullable: " << t->GetName());
            }
            setOpColumns.push_back(TInfoUnit(TString(t->GetName())));
        }

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        TVector<TExpression> joinFilters;

        for (const auto& iu : setOpColumns) {
            joinKeys.push_back(std::make_pair(iu, iu));
        }

        TString joinKind = "LeftSemi";
        if (setOpKind == "except") {
            joinKind = "LeftOnly";
        }

        result = MakeIntrusive<TOpJoin>(leftInput, rightInput, node->Pos(), joinKind, joinKeys, joinFilters);
    } else {
        Y_ENSURE(false, TStringBuilder() << "Unknow set operation: " << opSetOp.SetOp().StringValue());
    }

    if (setOpKind == "union" || setOpKind == "intersect" || setOpKind == "except") {
        auto itemType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        TVector<TInfoUnit> setOpColumns;
        for (const auto& t : itemType->GetItems()) {
            setOpColumns.push_back(TInfoUnit(TString(t->GetName())));
        }

        TVector<TOpAggregationTraits> opAggTraitsList;
        for (const auto& col : setOpColumns) {
            const auto originalColName = TInfoUnit(col);
            const auto aggFuncName = "distinct";
            const auto resultColName = TInfoUnit(col);
            TOpAggregationTraits opAggTraits(originalColName, aggFuncName, resultColName);
            opAggTraitsList.push_back(opAggTraits);
        }

        TVector<TInfoUnit> keyColumns;
        for (const auto& keyColumn : setOpColumns) {
            keyColumns.push_back(TInfoUnit(keyColumn));
        }

        result = MakeIntrusive<TOpAggregate>(result, opAggTraitsList, keyColumns, EOpPhase::Undefined, true, node->Pos());

    }
    return result;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpLimit(TExprNode::TPtr node) {
    const auto opLimit = TKqpOpLimit(node);
    const auto input = ExprNodeToOperator(opLimit.Input().Ptr());
    TExpression count(opLimit.Count().Ptr(), &Ctx);
    auto maybeOffset = opLimit.Offset();
    if (maybeOffset) {
        TExpression offset(maybeOffset.Cast().Ptr(), &Ctx);
        return MakeIntrusive<TOpLimit>(input, node->Pos(), count, offset, EOpPhase::Undefined);
    }
    return MakeIntrusive<TOpLimit>(input, node->Pos(), count, EOpPhase::Undefined);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpProject(TExprNode::TPtr node) {
    auto opProject = TKqpOpProject(node);
    return ExprNodeToOperator(opProject.Input().Ptr());
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpSort(TExprNode::TPtr node) {
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
        output = MakeIntrusive<TOpMap>(input, input->Pos, mapElements);
    }

    output = MakeIntrusive<TOpSort>(output, node->Pos(), sortElements);
    return output;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpAggregate(TExprNode::TPtr node) {
    const auto opAggregate = TKqpOpAggregate(node);
    const auto input = ExprNodeToOperator(opAggregate.Input().Ptr());

    TVector<TOpAggregationTraits> opAggTraitsList;
    for (const auto& traits : opAggregate.AggregationTraitsList()) {
        const auto originalColName = TInfoUnit(TString(traits.OriginalColName()));
        const auto aggFuncName = TString(traits.AggregationFunction());
        const auto resultColName = TInfoUnit(TString(traits.ResultColName()));
        TOpAggregationTraits opAggTraits(originalColName, aggFuncName, resultColName, GetDistinct(traits));
        opAggTraitsList.push_back(opAggTraits);
    }

    TVector<TInfoUnit> keyColumns;
    for (const auto& keyColumn : opAggregate.KeyColumns()) {
        keyColumns.push_back(TInfoUnit(TString(keyColumn)));
    }

    const bool distinctAll = opAggregate.DistinctAll() == "True" ? true : false;
    return MakeIntrusive<TOpAggregate>(input, opAggTraitsList, keyColumns, EOpPhase::Undefined, distinctAll, node->Pos());
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpReplaceAlias(TExprNode::TPtr node) {
    const auto input = ExprNodeToOperator(TKqpOpReplaceAlias(node).Input().Ptr());
    const auto inputIUs = input->GetOutputIUs();
    const auto* outputStructType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TVector<TMapElement> mapElements;
    TInfoUnitSet renamedSources;
    THashSet<TString> outputColumnNames;

    for (const auto& item : outputStructType->GetItems()) {
        const auto output = TInfoUnit(TString(item->GetName()));
        const auto source = ResolveVisibleIUByColumnName(inputIUs, TInfoUnit(output.GetColumnName()));
        Y_ENSURE(source, "Cannot resolve source column " << output.GetColumnName() << " for ReplaceAlias");

        mapElements.emplace_back(output, *source, node->Pos(), &Ctx, &PlanProps);
        renamedSources.insert(*source);
        outputColumnNames.insert(output.GetColumnName());
    }

    for (const auto& iu : inputIUs) {
        if (outputColumnNames.contains(iu.GetColumnName()) && !renamedSources.contains(iu)) {
            mapElements.emplace_back(MakeGeneratedIgnoreIU(PlanProps), iu, node->Pos(), &Ctx, &PlanProps);
        }
    }

    return MakeIntrusive<TOpMap>(input, node->Pos(), mapElements, true);
}

} // namespace NKikimr::Nkqp
