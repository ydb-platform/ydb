#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_utils.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <algorithm>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NNodes;

using DependencyPairType = std::pair<TInfoUnit, const TTypeAnnotationNode*>;

constexpr TStringBuf IgnoreArgPrefix = "__kqp_rbo_ignore_arg_";

bool ContainsIU(const TVector<TInfoUnit>& units, const TInfoUnit& unit) {
    return std::find(units.begin(), units.end(), unit) != units.end();
}

TInfoUnit MakeIgnoreIU(TPlanProps& props) {
    TStringBuilder name;
    name << IgnoreArgPrefix << props.InternalVarIdx++;
    return TInfoUnit(TString(name));
}

void ValidateUniqueOutputIUs(const TIntrusivePtr<IOperator>& op, TExprContext& ctx) {
    THashSet<TInfoUnit, TInfoUnit::THashFunction> seen;
    for (const auto& iu : op->GetOutputIUs()) {
        Y_ENSURE(!seen.contains(iu), "Duplicate visible column " << iu.GetFullName() << " after " << op->ToString(ctx));
        seen.insert(iu);
    }
}

void AddIgnoreRenameToMap(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& source, TExprContext& ctx, TPlanProps& props) {
    map->MapElements.emplace_back(MakeIgnoreIU(props), source, map->Pos, &ctx, &props);
}

TInfoUnit AddIgnoreRename(TIntrusivePtr<IOperator>& input, const TInfoUnit& source, TPositionHandle pos, TExprContext& ctx, TPlanProps& props) {
    const auto ignore = MakeIgnoreIU(props);

    if (input->Kind == EOperator::Map) {
        auto map = CastOperator<TOpMap>(input);
        if (ContainsIU(map->GetInput()->GetOutputIUs(), source)) {
            map->MapElements.emplace_back(ignore, source, map->Pos, &ctx, &props);
            return ignore;
        }
    }

    TVector<TMapElement> mapElements;
    mapElements.emplace_back(ignore, source, pos, &ctx, &props);
    input = MakeIntrusive<TOpMap>(input, pos, mapElements);
    return ignore;
}

void RenameJoinSideReferences(TOpJoin& join, const TInfoUnit& from, const TInfoUnit& to, bool rightSide) {
    for (auto& [leftKey, rightKey] : join.JoinKeys) {
        auto& key = rightSide ? rightKey : leftKey;
        if (key == from) {
            key = to;
        }
    }

    for (const auto& filter : join.JoinFilters) {
        const auto filterIUs = filter.GetInputIUs(false, true);
        Y_ENSURE(!ContainsIU(filterIUs, from), "Cannot normalize duplicate join output used by a join filter");
    }
}

std::optional<TInfoUnit> TryPromoteAppendToRename(const TIntrusivePtr<IOperator>& input, const TInfoUnit& source,
                                                  const TVector<TInfoUnit>& forbiddenOutputs) {
    if (input->Kind != EOperator::Map) {
        return std::nullopt;
    }

    auto map = CastOperator<TOpMap>(input);
    for (auto& mapElement : map->MapElements) {
        if (mapElement.IsRename() || !mapElement.IsColumnAccess() || mapElement.GetElementName() == source) {
            continue;
        }
        if (mapElement.GetColumnAccess() != source) {
            continue;
        }
        if (ContainsIU(forbiddenOutputs, mapElement.GetElementName())) {
            continue;
        }

        mapElement.SetIsRename(true);
        return mapElement.GetElementName();
    }

    return std::nullopt;
}

void NormalizeMap(const TIntrusivePtr<TOpMap>& map, TExprContext& ctx, TPlanProps& props) {
    const auto inputIUs = map->GetInput()->GetOutputIUs();
    THashSet<TInfoUnit, TInfoUnit::THashFunction> renameSources;

    for (const auto& mapElement : map->MapElements) {
        if (mapElement.IsRename()) {
            const auto source = mapElement.GetRename();
            Y_ENSURE(ContainsIU(inputIUs, source), "Rename source " << source.GetFullName() << " is not visible in map input");
            renameSources.insert(source);
        }
    }

    const size_t originalSize = map->MapElements.size();
    for (size_t i = 0; i < originalSize; ++i) {
        auto& mapElement = map->MapElements[i];
        const auto output = mapElement.GetElementName();

        if (!mapElement.IsRename() && mapElement.IsColumnAccess() && mapElement.GetColumnAccess() == output) {
            mapElement.SetIsRename(true);
            renameSources.insert(output);
            continue;
        }

        if (ContainsIU(inputIUs, output) && !renameSources.contains(output)) {
            AddIgnoreRenameToMap(map, output, ctx, props);
            renameSources.insert(output);
        }
    }

    ValidateUniqueOutputIUs(map, ctx);
}

void NormalizeJoin(const TIntrusivePtr<TOpJoin>& join, TExprContext& ctx, TPlanProps& props) {
    const bool leftVisible = join->JoinKind != "RightOnly" && join->JoinKind != "RightSemi";
    const bool rightVisible = join->JoinKind != "LeftOnly" && join->JoinKind != "LeftSemi";
    if (!leftVisible || !rightVisible) {
        ValidateUniqueOutputIUs(join, ctx);
        return;
    }

    auto leftOutput = join->GetLeftInput()->GetOutputIUs();
    auto rightOutput = join->GetRightInput()->GetOutputIUs();
    const auto conflicts = IUSetIntersect(leftOutput, rightOutput);

    for (const auto& conflict : conflicts) {
        TInfoUnit replacement;
        if (auto promoted = TryPromoteAppendToRename(join->GetRightInput(), conflict, leftOutput)) {
            replacement = *promoted;
        } else {
            replacement = AddIgnoreRename(join->GetRightInput(), conflict, join->Pos, ctx, props);
        }

        RenameJoinSideReferences(*join, conflict, replacement, true);
        rightOutput = join->GetRightInput()->GetOutputIUs();
        leftOutput = join->GetLeftInput()->GetOutputIUs();
    }

    ValidateUniqueOutputIUs(join, ctx);
}

void NormalizeUnionAll(const TIntrusivePtr<TOpUnionAll>& unionAll, TExprContext& ctx, TPlanProps& props) {
    const auto leftOutput = unionAll->GetLeftInput()->GetOutputIUs();
    const auto rightOutput = unionAll->GetRightInput()->GetOutputIUs();

    if (leftOutput == rightOutput) {
        ValidateUniqueOutputIUs(unionAll, ctx);
        return;
    }

    auto buildProject = [&](const TIntrusivePtr<IOperator>& input, const TVector<TInfoUnit>& output) {
        if (input->GetOutputIUs() == output) {
            return input;
        }

        TVector<TMapElement> mapElements;
        mapElements.reserve(output.size());
        for (const auto& iu : output) {
            mapElements.emplace_back(iu, iu, unionAll->Pos, &ctx, &props);
        }
        return CastOperator<IOperator>(MakeIntrusive<TOpMap>(input, unionAll->Pos, mapElements));
    };

    if (leftOutput.size() != rightOutput.size()) {
        THashSet<TInfoUnit, TInfoUnit::THashFunction> rightOutputSet;
        rightOutputSet.insert(rightOutput.begin(), rightOutput.end());
        TVector<TInfoUnit> commonOutput;
        commonOutput.reserve(std::min(leftOutput.size(), rightOutput.size()));
        for (const auto& iu : leftOutput) {
            if (rightOutputSet.contains(iu)) {
                commonOutput.push_back(iu);
            }
        }

        Y_ENSURE(!commonOutput.empty(),
            "UnionAll inputs have different column counts and no common columns: " << leftOutput.size() << " vs " << rightOutput.size());

        unionAll->GetLeftInput() = buildProject(unionAll->GetLeftInput(), commonOutput);
        unionAll->GetRightInput() = buildProject(unionAll->GetRightInput(), commonOutput);
        ValidateUniqueOutputIUs(unionAll, ctx);
        return;
    }

    TVector<TMapElement> mapElements;
    mapElements.reserve(leftOutput.size());
    THashSet<TInfoUnit, TInfoUnit::THashFunction> rightOutputSet;
    rightOutputSet.insert(rightOutput.begin(), rightOutput.end());
    for (size_t i = 0; i < leftOutput.size(); ++i) {
        const auto& source = rightOutputSet.contains(leftOutput[i]) ? leftOutput[i] : rightOutput[i];
        mapElements.emplace_back(leftOutput[i], source, unionAll->Pos, &ctx, &props);
    }

    unionAll->GetRightInput() = MakeIntrusive<TOpMap>(unionAll->GetRightInput(), unionAll->Pos, mapElements);
    ValidateUniqueOutputIUs(unionAll, ctx);
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
    auto maybeForceOptional = mapElement.ForceOptional();
    return maybeForceOptional && maybeForceOptional.Cast().StringValue() == "True";
}

bool GetOrdered(const TKqpOpMap& map) {
    auto maybeOrdered = map.Ordered();
    return maybeOrdered && maybeOrdered.Cast().StringValue() == "True";
}

} // anonymous namespace

void NormalizePlanOutputIUs(TOpRoot& root, TExprContext& ctx) {
    for (auto iter : root) {
        if (iter.Current->Kind == EOperator::Map) {
            NormalizeMap(CastOperator<TOpMap>(iter.Current), ctx, root.PlanProps);
        } else if (iter.Current->Kind == EOperator::Join) {
            NormalizeJoin(CastOperator<TOpJoin>(iter.Current), ctx, root.PlanProps);
        } else if (iter.Current->Kind == EOperator::UnionAll) {
            NormalizeUnionAll(CastOperator<TOpUnionAll>(iter.Current), ctx, root.PlanProps);
        } else {
            ValidateUniqueOutputIUs(iter.Current, ctx);
        }
    }

    const auto rootOutput = root.GetInput()->GetOutputIUs();
    for (const auto& column : root.ColumnOrder) {
        const auto iu = TInfoUnit(column);
        Y_ENSURE(ContainsIU(rootOutput, iu), "Root output column " << column << " is not visible before physical result narrowing");
    }
}

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
    opRoot->PlanProps.PgSyntax = std::stoi(kqpOpRoot.PgSyntax().StringValue());
 
    // We need to propagate plan properties reference into expressions in the plan
    for (auto it : *opRoot) {
        for (auto exprRef : it.Current->GetExpressions()) {
            exprRef.get().PlanProps = &(opRoot->PlanProps);
        }
    }

    opRoot->ComputeParents();
    NormalizePlanOutputIUs(*opRoot, Ctx);
    opRoot->ComputeParents();

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
    const auto ordered = GetOrdered(opMap);
    TVector<TMapElement> mapElements;

    for (const auto& mapElement : opMap.MapElements()) {
        const auto iu = TInfoUnit(mapElement.Variable().StringValue());
        if (mapElement.Maybe<TKqpOpMapElementRename>()) {
            auto element = mapElement.Cast<TKqpOpMapElementRename>();
            auto fromIU = TInfoUnit(element.From().StringValue());
            mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx, nullptr, false);
        } else {
            auto element = mapElement.Cast<TKqpOpMapElementLambda>();
            const auto forceOptional = GetForceOptional(element);
            // case lambda ($arg) { member $arg `name }
            if (auto maybeMember = element.Lambda().Body().Maybe<TCoMember>();
                !forceOptional && maybeMember && maybeMember.Cast().Struct().Ptr() == element.Lambda().Args().Arg(0).Ptr()) {
                auto member = maybeMember.Cast();
                auto name = member.Name().Cast<TCoAtom>();
                auto fromIU = TInfoUnit(name.StringValue());
                mapElements.emplace_back(iu, fromIU, node->Pos(), &Ctx, nullptr, false);
            } else {
                TExpression exprLambda(GetMapElementLambda(element.Lambda().Ptr(), forceOptional, Ctx), &Ctx);
                mapElements.emplace_back(iu, exprLambda);
            }
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

    return MakeIntrusive<TOpJoin>(leftInput, rightInput, node->Pos(), joinKind, joinKeys, joinFilters);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpUnionAll(TExprNode::TPtr node) {
    auto opUnionAll = TKqpOpUnionAll(node);
    auto leftInput = ExprNodeToOperator(opUnionAll.LeftInput().Ptr());
    auto rightInput = ExprNodeToOperator(opUnionAll.RightInput().Ptr());

    return MakeIntrusive<TOpUnionAll>(leftInput, rightInput, node->Pos());
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
        TOpAggregationTraits opAggTraits(originalColName, aggFuncName, resultColName);
        opAggTraitsList.push_back(opAggTraits);
    }

    TVector<TInfoUnit> keyColumns;
    for (const auto& keyColumn : opAggregate.KeyColumns()) {
        keyColumns.push_back(TInfoUnit(TString(keyColumn)));
    }

    const bool distinctAll = opAggregate.DistinctAll() == "True" ? true : false;
    return MakeIntrusive<TOpAggregate>(input, opAggTraitsList, keyColumns, EOpPhase::Undefined, distinctAll, node->Pos());
}

} // namespace NKikimr::NKqp
