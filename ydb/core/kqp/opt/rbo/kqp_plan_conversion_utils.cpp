#include "kqp_plan_conversion_utils.h"
#include "kqp_rbo_utils.h"
#include <util/generic/scope.h>

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

#include <algorithm>
#include <optional>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NNodes;

TInfoUnitId ResolveBinding(const TExpression::TBindings& bindings, TStringBuf name) {
    const auto it = bindings.find(TString(name));
    Y_ENSURE(it != bindings.end(), "Unknown input binding " << name);
    return it->second;
}

TOrderedIUs<> RebindProjection(const TOrderedIUs<>& projection, const TSubstitutions& substitutions) {
    TOrderedIUs<> result;
    for (const auto id : projection.Items()) {
        result.Append(Substitute(id, substitutions));
    }
    return result;
}


TUnorderedIUs GetVisibleDependencies(IOperator* op) {
    TUnorderedIUs result;
    for (const auto& item : IterateSubtree(op)) {
        if (item.Current->Kind == EOperator::AddDependencies) {
            result.UnionWith(CastOperator<TOpAddDependencies>(item.Current)->GetDependencies().Keys());
        } else if (item.Current->Kind == EOperator::Replicate) {
            const auto& rebindings = CastOperator<TOpReplicate>(item.Current)->GetRebindings();
            for (const auto& [source, output] : rebindings.Items()) {
                if (result.Contains(source)) {
                    result.Add(output);
                }
            }
        }
    }
    result.IntersectWith(op->GetOutputIUs());
    return result;
}

using TOuterTypes = TMappedIUs<const TTypeAnnotationNode*>;

void AddOuterType(TOuterTypes& types, TInfoUnitId id, const TTypeAnnotationNode* type) {
    if (const auto* previous = types.Find(id)) {
        Y_ENSURE(*previous == type, "Inconsistent captured IU type");
    } else {
        types.Add(id, type);
    }
}

// Normalize from inner scopes outward. A grandparent reference becomes
// outer -> caller-local capture -> callee-local capture, never a shared row ID.
TOuterTypes BindSubplanCaptures(IOperator& op, TPlanProps& props) {
    TOuterTypes outerTypes;
    // Insertions preserve the original nodes, but change their child edges.
    const TOpTraversal traversal(IterateSubtree(&op).begin());
    for (const auto& item : traversal) {
        auto& current = *item.Current;
        const auto subplans = current.GetSubplanIUs(props.Subplans);
        TOuterTypes required;
        for (const auto binding : subplans) {
            auto dependencies = BindSubplanCaptures(*props.Subplans.At(binding).Plan, props);
            props.Subplans.RefreshDependencies(binding);
            for (const auto& [outer, type] : dependencies.Items()) {
                AddOuterType(required, outer, type);
            }
        }

        auto missing = required.Keys();
        for (auto* child : current.GetChildren()) {
            missing.Subtract(child->GetOutputIUs());
        }
        if (!missing.Empty()) {
            // As in source InfuseDependents, captures are introduced below the
            // unary expression consumer. Multi-input placement is not inferred.
            Y_ENSURE(MatchOperator<IUnaryOperator>(current), "Cannot place forwarded captures below a multi-input expression");
            auto& consumer = CastOperator<IUnaryOperator>(current);
            // Extend an existing capture operator, reusing its capture of the same outer ID.
            auto* existing = consumer.GetInput()->Kind == EOperator::AddDependencies
                ? &CastOperator<TOpAddDependencies>(*consumer.GetInput()) : nullptr;
            TDependencyIUs captures = existing ? existing->GetDependencies() : TDependencyIUs{};
            TSubstitutions substitutions;
            for (const auto outer : missing) {
                std::optional<TInfoUnitId> local;
                for (const auto id : captures.Keys()) {
                    if (captures.Find(id)->Outer == outer) {
                        local = id;
                        break;
                    }
                }
                const auto* type = *required.Find(outer);
                if (!local) {
                    local = props.InfoUnitRegistry.AddCopy(outer);
                    captures.Add(*local, TCapturedIU{outer, type});
                }
                substitutions.Add(outer, *local);
                AddOuterType(outerTypes, outer, type);
            }
            if (existing) {
                existing->SetDependencies(std::move(captures));
            } else {
                consumer.SetInput(MakeIntrusive<TOpAddDependencies>(consumer.GetInput(), consumer.Pos, std::move(captures)));
            }

            for (const auto binding : subplans) {
                props.Subplans.RebindInputs(binding, substitutions);
            }
        }

        if (current.Kind == EOperator::AddDependencies) {
            for (const auto& [local, capture] : CastOperator<TOpAddDependencies>(current).GetDependencies().Items()) {
                AddOuterType(outerTypes, capture.Outer, capture.Type);
            }
        }
        current.Props.OutputIUs.reset();
    }
    return outerTypes;
}

bool GetForceOptional(const TKqpOpMapElementLambda& mapElement) {
    return mapElement.ForceOptional().StringValue() == "True";
}

bool GetProject(const TKqpOpMap& map) {
    return map.Project().IsValid();
}

bool GetDistinct(const TKqpOpAggregationTraits& aggTraits) {
    auto maybeDistinct = aggTraits.Distinct();
    return maybeDistinct && maybeDistinct.Cast().StringValue() == "distinct";
}

} // anonymous namespace

PlanConverter::TBindingScope PlanConverter::GetBindings(const TExprNode::TPtr& node) const {
    const auto it = OutputBindings.find(TImportKey{node.Get(), CaptureFreeNodes.at(node.Get()) ? 0 : BindingContext});
    Y_ENSURE(it != OutputBindings.end(), "Missing import binding scope for " << node->Content());
    return it->second;
}

std::optional<TOrderedIUs<>> PlanConverter::GetProjection(const TExprNode::TPtr& node) const {
    const auto it = Projections.find(TImportKey{node.Get(), CaptureFreeNodes.at(node.Get()) ? 0 : BindingContext});
    return it == Projections.end() ? std::nullopt : std::optional(it->second);
}

void PlanConverter::PropagateProjection(const TExprNode::TPtr& input, const TExprNode::TPtr& output,
    const TSubstitutions& substitutions)
{
    if (const auto projection = GetProjection(input)) {
        Projections[TImportKey{output.Get(), BindingContext}] = RebindProjection(*projection, substitutions);
    }
}

std::pair<TIntrusivePtr<IOperator>, PlanConverter::TBindingScope> PlanConverter::ConvertSubquery(
    TExprNode::TPtr node, const TExpression::TBindings& bindings)
{
    const auto parentContext = BindingContext;
    OuterBindings.push_back(&bindings);
    Y_DEFER {
        OuterBindings.pop_back();
        BindingContext = parentContext;
    };
    SubquerySources.insert(node.Get());
    BindingContext = CaptureFreeNodes.at(node.Get()) ? 0 : ++NextBindingContext;
    auto plan = ExprNodeToOperator(node);
    return {std::move(plan), GetBindings(node)};
}

TExpression PlanConverter::ConvertExpression(TExprNode::TPtr lambda, const TExpression::TBindings& bindings, bool allowSubqueries) {
    // Scalar Count/Offset nodes use the same one-row import boundary.
    if (!lambda->IsLambda()) {
        lambda = TExpression(lambda, &Ctx).GetLambda();
    }
    Y_ENSURE(lambda->Head().ChildrenSize() == 1);
    TNodeOnNodeOwnedMap replacements;
    VisitExpr(lambda->ChildPtr(1), [&](const TExprNode::TPtr& node) {
        if (!TKqpSublinkBase::Match(node.Get())) {
            return true;
        }
        Y_ENSURE(allowSubqueries, "Subqueries are supported only in filter and projection expressions");

        // Convert the producer before allocating its result binding. The binder
        // replaces the sublink node directly.
        const auto source = TKqpSublinkBase(node).Subquery();
        auto [subplan, subplanBindings] = ConvertSubquery(source.Ptr(), bindings);
        ESubplanType type;
        TOrderedIUs<> tuple;
        std::optional<TInfoUnitId> resultIU;
        if (TKqpExprSublink::Match(node.Get())) {
            type = ESubplanType::EXPR;
        } else if (TKqpExistsSublink::Match(node.Get())) {
            type = ESubplanType::EXISTS;
        } else {
            type = ESubplanType::IN_SUBPLAN;
            const auto& inLambda = node->Child(TKqpInSublink::idx_InLambda);
            Y_ENSURE(inLambda->IsLambda());
            const auto& comparison = inLambda->Child(1);
            Y_ENSURE(comparison->IsCallable("==") && comparison->Head().IsCallable("Member"),
                "Only a single column reference in the IN clause is supported");
            const TString name(comparison->Head().Tail().Content());
            auto it = bindings.find(name);
            // IN's outer-row schema may retain the SQL alias encoding. Prefer
            // an exact field first, so literal dots/prefixes remain unambiguous.
            if (it == bindings.end() && name.StartsWith("_alias_")) {
                const auto [alias, column] = SplitAliasedMemberName(name);
                it = bindings.find(alias + "." + column);
            }
            Y_ENSURE(it != bindings.end(), "Unknown IN binding " << name);
            tuple.Append(it->second);
        }

        if (type != ESubplanType::EXISTS) {
            // Match YQL's scalar/single-column IN contract at the import boundary.
            // Never infer the result from optimizer outputs or generated labels.
            const auto* row = source.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            Y_ENSURE(row->GetSize(), "Subplan has no result column");
            resultIU = ResolveBinding(*subplanBindings, row->GetItems().front()->GetName());
        }
        const auto id = PlanProps.InfoUnitRegistry.AddGenerated("subquery");
        PlanProps.Subplans.Add(id, std::move(subplan), type, std::move(tuple), resultIU);
        // clang-format off
        auto member = Build<TCoMember>(Ctx, node->Pos())
            .Struct(lambda->Head().HeadPtr())
            .Name<TCoAtom>().Value(Ctx.GetIndexAsString(id)).Build()
            .Done().Ptr();
        // clang-format on
        replacements.emplace(node.Get(), std::move(member));
        return false; // Subquery expressions have their own conversion scope.
    });
    return TExpression::FromExpr(std::move(lambda), bindings, Ctx, PlanProps, std::move(replacements));
}

TIntrusivePtr<TOpRoot> PlanConverter::ConvertRoot(TExprNode::TPtr node, TExprNode::TPtr queryColumnsList) {
    CountUses(node);
    auto kqpOpRoot = TKqpOpRoot(node);
    auto rootInput = ExprNodeToOperator(kqpOpRoot.Input().Ptr());
    const auto bindings = GetBindings(kqpOpRoot.Input().Ptr());
    TOrderedIUs<TString> columns;
    for (const auto& column : kqpOpRoot.ColumnOrder()) {
        columns.Append(ResolveBinding(*bindings, column.Value()), column.StringValue());
    }
    TVector<TString> queryColumns;
    if (queryColumnsList) {
        for (const auto& column : queryColumnsList->Children()) {
            queryColumns.emplace_back(column->Content());
        }
    }
    auto opRoot = MakeIntrusive<TOpRoot>(
        std::move(rootInput), node->Pos(), std::move(columns), std::move(queryColumns));
    opRoot->Node = node;
    opRoot->PlanProps = std::move(PlanProps);
 
    // We need to propagate plan properties reference into expressions in the plan
    for (const auto& it : *opRoot) {
        it.Current->BindExpressionPlanProps(&opRoot->PlanProps);
    }

    Y_ENSURE(BindSubplanCaptures(*opRoot, opRoot->PlanProps).Keys().Empty(), "Unbound outer captures in root plan");

   return opRoot;
}

void PlanConverter::CountUses(const TExprNode::TPtr& root) {
    Uses.clear();
    // A capture-free subtree has the same bindings in every lexical context,
    // including when used inside a correlated subquery. Compute this once per
    // AST node; rescanning each imported subtree would be quadratic.
    CaptureFreeNodes.clear();
    VisitExpr(root, [](const TExprNode::TPtr&) { return true; }, [&](const TExprNode::TPtr& node) {
        bool captureFree = !(TKqpInfuseDependents::Match(node.Get()) && TKqpInfuseDependents(node).Columns().Size());
        for (const auto& child : node->Children()) {
            captureFree = captureFree && CaptureFreeNodes.at(child.Get());
        }
        CaptureFreeNodes.emplace(node.Get(), captureFree);
        return true;
    });
    TVector<const TExprNode*> pending{root.Get()};
    THashMap<const TExprNode*, size_t> groupingDefinitions;
    Uses[root.Get()] = 1;
    for (size_t index = 0; index < pending.size(); ++index) {
        if (TKqpOpGroupingSets::Match(pending[index])) {
            ++groupingDefinitions[pending[index]->Child(TKqpOpGroupingSets::idx_Input)];
        }
        const auto& node = *pending[index];
        for (ui32 childIndex = 0; childIndex < node.ChildrenSize(); ++childIndex) {
            // These references supply a row schema for annotation, not another
            // consumer of the input plan. Its owning operator visits it below.
            if ((childIndex == 0 && (TKqpOpMapElementRename::Match(&node) ||
                    TKqpOpMapElementLambda::Match(&node) || TKqpOpSortElement::Match(&node))) ||
                (childIndex < 2 && TKqpOpJoinFilter::Match(&node))) {
                continue;
            }
            const auto& child = node.ChildPtr(childIndex);
            if (++Uses[child.Get()] == 1) {
                pending.push_back(child.Get());
            }
        }
    }
    for (const auto& [aggregate, groupingUses] : groupingDefinitions) {
        Y_ENSURE(TKqpOpAggregate::Match(aggregate));
        // Each GroupingSets owns an Aggregate definition; an ordinary consumer
        // also needs the standalone aggregate. Account for their shared source.
        const auto instances = groupingUses + (Uses.at(aggregate) > groupingUses);
        Uses[aggregate->Child(TKqpOpAggregate::idx_Input)] += instances - 1;
    }
}

TIntrusivePtr<IOperator> PlanConverter::ExprNodeToOperator(TExprNode::TPtr node) {
    if (Uses.empty()) {
        CountUses(node);
    }
    const auto parentContext = BindingContext;
    if (CaptureFreeNodes.at(node.Get())) {
        BindingContext = 0;
    }
    Y_DEFER { BindingContext = parentContext; };
    const TImportKey key{node.Get(), BindingContext};
    if (const auto it = Converted.find(key); it != Converted.end()) {
        auto port = it->second.Hub->AddOutput();
        auto bindings = std::make_shared<TExpression::TBindings>(*it->second.Bindings);
        const auto& rebindings = port->GetRebindings();
        for (auto& [name, id] : *bindings) {
            id = *rebindings.Find(id);
        }
        if (it->second.Projection) {
            Projections[key] = RebindProjection(*it->second.Projection, rebindings);
        }
        OutputBindings[key] = std::move(bindings);
        return port;
    }
    Y_ENSURE(Imported.insert(key).second, "Repeated import without a shared source edge");

    TIntrusivePtr<IOperator> result;
    if (NYql::NNodes::TKqpOpEmptySource::Match(node.Get())) {
        result = ConvertTKqpOpEmptySource(node);
    } else if (NYql::NNodes::TKqpOpRead::Match(node.Get())) {
        auto read = TOpRead::FromExpr(node, PlanProps.InfoUnitRegistry);
        auto bindings = std::make_shared<TExpression::TBindings>();
        const auto source = TKqpOpRead(node);
        bindings->reserve(source.Columns().Size());
        TOrderedIUs<> projection;
        auto id = read->GetColumns().begin();
        for (const auto& column : source.Columns()) {
            // FromExpr allocates fresh consecutive IDs in source-column order.
            const auto name = TInfoUnit(source.Alias().StringValue(), column.StringValue()).GetFullName();
            projection.Append(*id);
            Y_ENSURE(bindings->emplace(name, *id++).second, "Ambiguous Read binding " << name);
        }
        Projections[key] = std::move(projection);
        OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(bindings);
        result = std::move(read);
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
    } else if (NYql::NNodes::TKqpOpGroupingSets::Match(node.Get())) {
        result = ConvertTKqpOpGroupingSets(node);
    } else if (NYql::NNodes::TKqpOpWindow::Match(node.Get())) {
        result = ConvertTKqpOpWindow(node);
    } else if (NYql::NNodes::TKqpOpReplaceAlias::Match(node.Get())) {
        result = ConvertTKqpOpReplaceAlias(node);
    } else if (NYql::NNodes::TKqpOpReplaceColumns::Match(node.Get())) {
        result = ConvertTKqpOpReplaceColumns(node);
    } else if (NYql::NNodes::TKqpOpTableEffect::Match(node.Get())){
        result = ConvertTKqpOpTableEffect(node);
    } else {
        YQL_ENSURE(false, "Unknown operator node");
    }
    // A sublink can have several callers despite one structural use. Share
    // its capture-free producer, including the boundary below local captures.
    const bool sharedBoundary = CaptureFreeNodes.at(node.Get())
        && (parentContext != 0 || SubquerySources.contains(node.Get()));
    if (Uses.Value(node.Get(), 0) > 1 || sharedBoundary) {
        auto hub = TReplicate::Create(std::move(result), node->Pos(), PlanProps.InfoUnitRegistry);
        result = hub->AddOutput();
        Converted.emplace(key, TSharedImport{std::move(hub), GetBindings(node), GetProjection(node)});
    }
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
    const auto source = TKqpOpMap(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    const bool sourceProjects = GetProject(source);
    auto projection = sourceProjects ? std::optional<TOrderedIUs<>>(std::in_place) : GetProjection(source.Input().Ptr());
    TVector<std::pair<TString, TMapElement>> definitions;
    for (const auto& item : source.MapElements()) {
        TExpression expression;
        if (const auto rename = item.Maybe<TKqpOpMapElementRename>()) {
            expression = MakeColumnAccess(ResolveBinding(*bindings, rename.Cast().From().Value()),
                item.Pos(), &Ctx, &PlanProps);
        } else {
            const auto element = item.Cast<TKqpOpMapElementLambda>();
            expression = ConvertExpression(GetMapElementLambda(element.Lambda().Ptr(), GetForceOptional(element), Ctx), *bindings);
        }
        definitions.emplace_back(item.Variable().StringValue(), TMapElement(std::move(expression)));
    }

    // Bind all RHS expressions against the original scope, then allocate the
    // output definitions consecutively. Swaps never see partially renamed input.
    // Source projection restricts name visibility, not the optimizer's available
    // IUs. Unmentioned inputs still pass through the logical Map under their IDs.
    auto output = std::make_shared<TExpression::TBindings>();
    if (!sourceProjects) {
        *output = *bindings;
    }
    TMapIUs elements;
    for (auto& [name, element] : definitions) {
        const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
        elements.Add(id, std::move(element));
        if (projection) {
            projection->Append(id);
        }
        Y_ENSURE(output->emplace(name, id).second, "Duplicate Map output " << name);
    }
    if (sourceProjects) {
        // Captures remain visible to nested expressions. They already pass
        // through the append-only Map; no identity definition is needed.
        const auto dependencies = GetVisibleDependencies(input.get());
        for (const auto& [name, id] : *bindings) {
            if (dependencies.Contains(id) && !output->contains(name)) {
                output->emplace(name, id);
            }
        }
    }
    if (projection) {
        Projections[TImportKey{node.Get(), BindingContext}] = std::move(*projection);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpMap>(std::move(input), node->Pos(), std::move(elements));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpInfuseDependents(TExprNode::TPtr node) {
    const auto source = TKqpInfuseDependents(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    auto output = std::make_shared<TExpression::TBindings>(*GetBindings(source.Input().Ptr()));
    Y_ENSURE(source.Columns().Size() == source.Types().Size());
    TDependencyIUs dependencies;
    TSubstitutions locals;
    for (size_t i = 0; i < source.Columns().Size(); ++i) {
        const auto name = source.Columns().Item(i).StringValue();
        std::optional<TInfoUnitId> id;
        for (auto scope = OuterBindings.rbegin(); scope != OuterBindings.rend() && !id; ++scope) {
            auto found = (*scope)->find(name);
            if (found == (*scope)->end() && name.StartsWith("_alias_")) {
                const auto [alias, column] = SplitAliasedMemberName(name);
                found = (*scope)->find(alias + "." + column);
            }
            if (found != (*scope)->end()) {
                id = found->second;
            }
        }
        Y_ENSURE(id, "Unknown captured binding " << name);
        const auto* type = source.Types().Item(i).Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        TInfoUnitId local;
        if (const auto* existing = locals.Find(*id)) {
            local = *existing;
            Y_ENSURE(dependencies.Find(local)->Type == type, "Inconsistent captured IU type");
        } else {
            local = PlanProps.InfoUnitRegistry.AddCopy(*id);
            locals.Add(*id, local);
            dependencies.Add(local, TCapturedIU{*id, type});
        }
        const auto [it, inserted] = output->emplace(name, local);
        Y_ENSURE(inserted || it->second == local, "Captured binding shadows an inner field " << name);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    PropagateProjection(source.Input().Ptr(), node);
    return MakeIntrusive<TOpAddDependencies>(std::move(input), node->Pos(), std::move(dependencies));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpFilter(TExprNode::TPtr node) {
    auto opFilter = TKqpOpFilter(node);
    auto input = ExprNodeToOperator(opFilter.Input().Ptr());
    const auto bindings = GetBindings(opFilter.Input().Ptr());
    PropagateProjection(opFilter.Input().Ptr(), node);
    auto expression = ConvertExpression(opFilter.Lambda().Ptr(), *bindings);
    auto filter = MakeIntrusive<TOpFilter>(std::move(input), node->Pos(), expression);
    OutputBindings[TImportKey{node.Get(), BindingContext}] = bindings;
    return filter;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpJoin(TExprNode::TPtr node) {
    const auto source = TKqpOpJoin(node);
    auto left = ExprNodeToOperator(source.LeftInput().Ptr());
    const auto leftBindings = GetBindings(source.LeftInput().Ptr());
    auto right = ExprNodeToOperator(source.RightInput().Ptr());
    const auto rightBindings = *GetBindings(source.RightInput().Ptr());
    Y_ENSURE(!left->GetOutputIUs().HasAny(right->GetOutputIUs()),
        "Shared Join inputs must already use distinct Replicate ports");

    TPairedIUs keys;
    for (const auto& key : source.JoinKeys()) {
        keys.Add(ResolveBinding(*leftBindings, TInfoUnit(key.LeftLabel().StringValue(), key.LeftColumn().StringValue()).GetFullName()),
            ResolveBinding(rightBindings, TInfoUnit(key.RightLabel().StringValue(), key.RightColumn().StringValue()).GetFullName()));
    }
    const auto kind = source.JoinKind().StringValue();
    // When the Join outputs both sides, a right field spelled like a left one
    // is renamed apart: the output spelling keeps the left field, while Join
    // filters read the right field.
    const bool renamesRightConflicts = JoinOutputsLeft(kind) && JoinOutputsRight(kind);
    const auto merge = [](TExpression::TBindings& into, const TExpression::TBindings& from, bool replace) {
        for (const auto& [name, id] : from) {
            const auto [it, inserted] = into.emplace(name, id);
            if (!inserted && replace) {
                it->second = id;
            }
        }
    };
    TVector<TExpression> filters;
    if (source.JoinFilters().Size()) {
        auto filterBindings = *leftBindings;
        merge(filterBindings, rightBindings, renamesRightConflicts);
        for (const auto& filter : source.JoinFilters()) {
            filters.push_back(ConvertExpression(filter.Lambda().Ptr(), filterBindings, /*allowSubqueries=*/false));
        }
    }
    auto output = std::make_shared<TExpression::TBindings>();
    if (JoinOutputsLeft(kind)) {
        merge(*output, *leftBindings, false);
    }
    if (JoinOutputsRight(kind)) {
        merge(*output, rightBindings, false);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpJoin>(std::move(left), std::move(right), node->Pos(), kind, std::move(keys), filters);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpSetOp(TExprNode::TPtr node) {
    const auto source = TKqpOpSetOp(node);
    auto leftNode = source.LeftInput().Ptr();
    auto rightNode = source.RightInput().Ptr();
    auto left = ExprNodeToOperator(leftNode);
    const auto originalLeftBindings = GetBindings(leftNode);
    const auto leftProjection = GetProjection(leftNode);
    auto leftBindings = *originalLeftBindings;
    auto right = ExprNodeToOperator(rightNode);
    auto rightBindings = *GetBindings(rightNode);
    const auto fields = [](const TExprNode::TPtr& expr) {
        return expr->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems();
    };
    const auto outputFields = fields(node);
    const auto leftFields = fields(leftNode);
    const auto rightFields = fields(rightNode);
    Y_ENSURE(outputFields.size() == leftFields.size() && outputFields.size() == rightFields.size());
    // Widen on this edge only; never mutate a memoized producer's source AST.
    // Only a direct Map input is widened, as if its elements were forced
    // optional; other producers keep their types.
    const auto widen = [&](TIntrusivePtr<IOperator> child, const TExprNode::TPtr& sourceNode, const auto& fields,
                           TExpression::TBindings& bindings) -> TIntrusivePtr<IOperator> {
        if (!TKqpOpMap::Match(sourceNode.Get())) {
            return child;
        }
        TMapIUs definitions;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (outputFields[i]->GetItemType()->IsOptionalOrNull() && !fields[i]->GetItemType()->IsOptionalOrNull()) {
                const auto inputId = ResolveBinding(bindings, fields[i]->GetName());
                const auto id = PlanProps.InfoUnitRegistry.AddCopy(inputId);
                definitions.Add(id, MakeUnaryCallable("Just", MakeColumnAccess(inputId, node->Pos(), &Ctx, &PlanProps)));
                bindings.at(TString(fields[i]->GetName())) = id;
            }
        }
        if (definitions.Keys().Empty()) {
            return child;
        }
        return MakeIntrusive<TOpMap>(std::move(child), node->Pos(), std::move(definitions));
    };
    left = widen(std::move(left), leftNode, leftFields, leftBindings);
    right = widen(std::move(right), rightNode, rightFields, rightBindings);
    const auto kind = source.SetOp().StringValue();
    auto output = std::make_shared<TExpression::TBindings>();
    TIntrusivePtr<IOperator> result;

    if (kind == "union_all" || kind == "union") {
        TUnionAllIUs columns(TUnionInputPolicy{2});
        for (size_t i = 0; i < outputFields.size(); ++i) {
            const TString name(outputFields[i]->GetName());
            const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
            TUnionInputRow row;
            row.Inputs = {ResolveBinding(leftBindings, leftFields[i]->GetName()),
                          ResolveBinding(rightBindings, rightFields[i]->GetName())};
            columns.Add(id, std::move(row));
            output->emplace(name, id);
        }
        result = MakeIntrusive<TOpUnionAll>(std::move(left), std::move(right), node->Pos(), std::move(columns));
    } else if (kind == "intersect" || kind == "except") {
        TMapIUs leftPickles;
        TMapIUs rightPickles;
        TPairedIUs keys;
        for (size_t i = 0; i < outputFields.size(); ++i) {
            auto leftId = ResolveBinding(leftBindings, leftFields[i]->GetName());
            auto rightId = ResolveBinding(rightBindings, rightFields[i]->GetName());
            output->emplace(TString(outputFields[i]->GetName()), leftId);
            // Use stable pickle for nullable columns
            if (outputFields[i]->GetItemType()->IsOptionalOrNull()) {
                const auto pickle = [&](TInfoUnitId input, TMapIUs& definitions) {
                    const auto id = PlanProps.InfoUnitRegistry.AddGenerated("null_safe_key");
                    definitions.Add(id, MakeUnaryCallable("StablePickle", MakeColumnAccess(input, node->Pos(), &Ctx, &PlanProps)));
                    return id;
                };
                leftId = pickle(leftId, leftPickles);
                rightId = pickle(rightId, rightPickles);
            }
            keys.Add(leftId, rightId);
        }
        if (!leftPickles.Keys().Empty()) {
            left = MakeIntrusive<TOpMap>(std::move(left), node->Pos(), std::move(leftPickles));
            right = MakeIntrusive<TOpMap>(std::move(right), node->Pos(), std::move(rightPickles));
        }
        result = MakeIntrusive<TOpJoin>(std::move(left), std::move(right), node->Pos(),
            kind == "except" ? "LeftOnly" : "LeftSemi", std::move(keys));
    } else {
        Y_ENSURE(false, "Unsupported set operation " << kind);
    }

    if (kind != "union_all") {
        TOrderedIUs<> keys;
        TAggregationIUs aggregations;
        for (const auto* field : outputFields) {
            const TString name(field->GetName());
            const auto inputId = ResolveBinding(*output, name);
            keys.Append(inputId);
            const auto resultId = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
            aggregations.Add(resultId, TOpAggregationTraits{inputId, "distinct"});
            output->at(name) = resultId;
        }
        result = MakeIntrusive<TOpAggregate>(std::move(result), std::move(aggregations), std::move(keys),
            EOpPhase::Undefined, true, node->Pos());
    }
    if (leftProjection) {
        TSubstitutions substitutions;
        for (size_t i = 0; i < outputFields.size(); ++i) {
            substitutions.Add(ResolveBinding(*originalLeftBindings, leftFields[i]->GetName()),
                ResolveBinding(*output, outputFields[i]->GetName()));
        }
        Projections[TImportKey{node.Get(), BindingContext}] = RebindProjection(*leftProjection, substitutions);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return result;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpLimit(TExprNode::TPtr node) {
    const auto source = TKqpOpLimit(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    auto count = ConvertExpression(source.Count().Ptr(), *bindings, /*allowSubqueries=*/false);
    std::optional<TExpression> offset;
    if (const auto value = source.Offset()) {
        offset = ConvertExpression(value.Cast().Ptr(), *bindings, /*allowSubqueries=*/false);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = bindings;
    PropagateProjection(source.Input().Ptr(), node);
    return MakeIntrusive<TOpLimit>(std::move(input), node->Pos(), TPhysicalOpProps{}, count, std::move(offset), EOpPhase::Undefined);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpProject(TExprNode::TPtr node) {
    const auto source = TKqpOpProject(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    auto output = std::make_shared<TExpression::TBindings>();
    TOrderedIUs<> projection;
    for (const auto& column : node->Child(TKqpOpProject::idx_ProjectList)->Children()) {
        const TString name(column->Content());
        const auto id = ResolveBinding(*bindings, name);
        output->emplace(name, id);
        projection.Append(id);
    }
    Projections[TImportKey{node.Get(), BindingContext}] = std::move(projection);
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    // A source visibility boundary must not become an optimizer projection.
    return input;
}

TSortIUs PlanConverter::ConvertSortKeys(const TKqpOpSortList& source,
    const TExpression::TBindings& bindings, TMapIUs& definitions)
{
    TSortIUs keys;
    for (const auto& item : source) {
        auto expression = ConvertExpression(item.Lambda().Ptr(), bindings, /*allowSubqueries=*/false);
        TInfoUnitId id;
        if (expression.IsColumnAccess()) {
            id = *expression.GetRawInputIUs().begin();
        } else {
            id = PlanProps.InfoUnitRegistry.AddGenerated("sort_key");
            definitions.Add(id, std::move(expression));
        }
        keys.Append(id, TSortOrder{item.Direction().StringValue() == "asc", item.NullsFirst().StringValue() == "first"});
    }
    return keys;
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpSort(TExprNode::TPtr node) {
    const auto source = TKqpOpSort(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    TMapIUs definitions;
    auto keys = ConvertSortKeys(source.SortExpressions(), *bindings, definitions);
    if (!definitions.Keys().Empty()) {
        input = MakeIntrusive<TOpMap>(std::move(input), node->Pos(), std::move(definitions));
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = bindings;
    PropagateProjection(source.Input().Ptr(), node);
    return MakeIntrusive<TOpSort>(std::move(input), node->Pos(), std::move(keys));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpAggregate(TExprNode::TPtr node) {
    const auto source = TKqpOpAggregate(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    auto output = std::make_shared<TExpression::TBindings>();
    const bool distinctAll = source.DistinctAll() == "True";
    TOrderedIUs<> keys;
    for (const auto& key : source.KeyColumns()) {
        const TString name(key.Value());
        const auto id = ResolveBinding(*bindings, name);
        keys.Append(id);
        if (!distinctAll) {
            output->emplace(name, id);
        }
    }
    TAggregationIUs aggregations;
    for (const auto& traits : source.AggregationTraitsList()) {
        const TString name(traits.ResultColName().Value());
        const auto inputId = ResolveBinding(*bindings, traits.OriginalColName().Value());
        const auto resultId = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
        aggregations.Add(resultId, TOpAggregationTraits{inputId, TString(traits.AggregationFunction()), GetDistinct(traits)});
        Y_ENSURE(output->emplace(name, resultId).second, "Duplicate Aggregate output " << name);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    Y_ENSURE(distinctAll || !keys.Items().empty() || !aggregations.Keys().Empty(),
        "Scalar aggregation without aggregation traits is not supported");
    return MakeIntrusive<TOpAggregate>(std::move(input), std::move(aggregations), std::move(keys),
        EOpPhase::Undefined, distinctAll, node->Pos());
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpGroupingSets(TExprNode::TPtr node) {
    const auto source = TKqpOpGroupingSets(node);
    // This Aggregate supplies the grouping definition, not a consumed result
    // stream. Give the wrapper its own definition; its input can still fan out.
    auto input = ConvertTKqpOpAggregate(source.Input().Ptr());
    Y_ENSURE(MatchOperator<TOpAggregate>(input), "Grouping sets input must be an aggregate");
    const auto bindings = GetBindings(source.Input().Ptr());
    TVector<TUnorderedIUs> sets;
    for (const auto& group : source.GroupingSets()) {
        TUnorderedIUs keys;
        for (const auto& key : group) {
            keys.Add(ResolveBinding(*bindings, key.Value()));
        }
        sets.push_back(std::move(keys));
    }
    THashMap<TString, TInfoUnitId> indicatorKeys;
    for (const auto& indicator : source.GroupingIndicators()) {
        Y_ENSURE(indicator.Size() == 2, "Grouping indicator must be a pair of a group by key and a column");
        Y_ENSURE(indicatorKeys.emplace(indicator.Item(1).StringValue(),
            ResolveBinding(*bindings, indicator.Item(0).StringValue())).second, "Duplicate grouping indicator");
    }
    TMappedIUs<TInfoUnitId> columns;
    TMappedIUs<TInfoUnitId> indicators;
    auto output = std::make_shared<TExpression::TBindings>();
    const auto* schema = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto* field : schema->GetItems()) {
        const TString name(field->GetName());
        TInfoUnitId id;
        if (const auto it = indicatorKeys.find(name); it != indicatorKeys.end()) {
            id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
            indicators.Add(id, it->second);
        } else {
            const auto sourceId = ResolveBinding(*bindings, name);
            id = PlanProps.InfoUnitRegistry.AddCopy(sourceId);
            columns.Add(id, sourceId);
        }
        Y_ENSURE(output->emplace(name, id).second, "Duplicate GroupingSets output " << name);
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpGroupingSets>(CastOperator<TOpAggregate>(input),
        std::move(sets), std::move(columns), node->Pos(), std::move(indicators));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpWindow(TExprNode::TPtr node) {
    const auto source = TKqpOpWindow(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    TOrderedIUs<> partitionKeys;
    for (const auto& key : source.PartitionKeys()) {
        partitionKeys.Append(ResolveBinding(*bindings, key.Value()));
    }
    TMapIUs definitions;
    auto sortKeys = ConvertSortKeys(source.SortExpressions(), *bindings, definitions);
    if (!definitions.Keys().Empty()) {
        input = MakeIntrusive<TOpMap>(std::move(input), node->Pos(), std::move(definitions));
    }

    auto output = std::make_shared<TExpression::TBindings>(*bindings);
    TWindowIUs functions;
    for (const auto& item : source.WindowFuncs()) {
        TOrderedIUs<> arguments;
        for (const auto& argument : item.Arguments()) {
            arguments.Append(ResolveBinding(*bindings, argument.Value()));
        }
        const TString name(item.ResultColName().Value());
        const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
        functions.Add(id, TOpWindowFunc{TString(item.Function()), WindowFuncKindFromString(TString(item.Kind())), std::move(arguments)});
        Y_ENSURE(output->emplace(name, id).second, "Duplicate Window output " << name);
    }
    const auto sourceFrame = source.Frame();
    TOpWindowFrame frame;
    frame.Type = WindowFrameTypeFromString(TString(sourceFrame.FrameType()));
    frame.BeginKind = WindowFrameBoundFromString(TString(sourceFrame.BeginKind()));
    frame.BeginValue = FromString<ui64>(TString(sourceFrame.BeginValue()));
    frame.EndKind = WindowFrameBoundFromString(TString(sourceFrame.EndKind()));
    frame.EndValue = FromString<ui64>(TString(sourceFrame.EndValue()));
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpWindow>(std::move(input), node->Pos(), std::move(functions), std::move(partitionKeys), std::move(sortKeys), frame);
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpReplaceAlias(TExprNode::TPtr node) {
    const auto source = TKqpOpReplaceAlias(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto bindings = GetBindings(source.Input().Ptr());
    const auto* schema = source.Input().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto output = std::make_shared<TExpression::TBindings>();
    TMapIUs elements;
    TSubstitutions substitutions;
    for (const auto* field : schema->GetItems()) {
        const TString oldName(field->GetName());
        // Match the import AST's ReplaceAlias spelling rule exactly.
        const auto dot = oldName.find('.');
        const auto name = source.Alias().StringValue() + "." + oldName.substr(dot == TString::npos ? 0 : dot + 1);
        const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
        const auto inputId = ResolveBinding(*bindings, oldName);
        elements.Add(id, MakeColumnAccess(inputId, node->Pos(), &Ctx, &PlanProps));
        substitutions.Add(inputId, id);
        Y_ENSURE(output->emplace(name, id).second, "Ambiguous ReplaceAlias output " << name);
    }
    PropagateProjection(source.Input().Ptr(), node, substitutions);
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpMap>(std::move(input), node->Pos(), std::move(elements));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpReplaceColumns(TExprNode::TPtr node) {
    const auto source = TKqpOpReplaceColumns(node);
    auto input = ExprNodeToOperator(source.Input().Ptr());
    const auto inputProjection = GetProjection(source.Input().Ptr());
    const auto columns = node->ChildPtr(TKqpOpReplaceColumns::idx_Columns);
    Y_ENSURE(inputProjection, "Missing projection for positional column replacement");
    Y_ENSURE(inputProjection->Items().size() == columns->ChildrenSize());
    auto output = std::make_shared<TExpression::TBindings>();
    TOrderedIUs<> projection;
    TMapIUs elements;
    for (size_t i = 0; i < columns->ChildrenSize(); ++i) {
        const TString name(columns->Child(i)->Content());
        const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit(name));
        elements.Add(id, MakeColumnAccess(inputProjection->Items()[i], node->Pos(), &Ctx, &PlanProps));
        projection.Append(id);
        Y_ENSURE(output->emplace(name, id).second, "Duplicate ReplaceColumns output " << name);
    }
    Projections[TImportKey{node.Get(), BindingContext}] = std::move(projection);
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpMap>(std::move(input), node->Pos(), std::move(elements));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpEmptySource(TExprNode::TPtr node) {
    auto bindings = std::make_shared<TExpression::TBindings>();
    TUnorderedIUs columns;
    if (node->ChildrenSize()) {
        const auto* schema = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        for (const auto* field : schema->GetItems()) {
            const TString name(field->GetName());
            const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit("", name));
            columns.Add(id);
            Y_ENSURE(bindings->emplace(name, id).second, "Duplicate parameter-table column " << name);
        }
        if (const auto order = TypeCtx.LookupColumnOrder(node->Head())) {
            TOrderedIUs<> projection;
            for (const auto& column : *order) {
                projection.Append(ResolveBinding(*bindings, column.PhysicalName));
            }
            Projections[TImportKey{node.Get(), BindingContext}] = std::move(projection);
        }
    } else {
        Projections[TImportKey{node.Get(), BindingContext}] = {};
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(bindings);
    return MakeIntrusive<TOpEmptySource>(node->Pos(), node->ChildrenSize() ? node->HeadPtr() : nullptr, std::move(columns));
}

TIntrusivePtr<IOperator> PlanConverter::ConvertTKqpOpTableEffect(TExprNode::TPtr node) {
    const auto opTableEffect = TKqpOpTableEffect(node);
    auto input = ExprNodeToOperator(opTableEffect.Input().Ptr());
    
    PlanProps.WithEffects = true;

    EEffectType type = EEffectType::InsertRows;
    TEffectOptions options;

    auto effectType = opTableEffect.EffectType().StringValue();

    auto processColumns = [](const TCoAtomList& atomList) {
        TVector<TString> result;
        for (auto a : atomList) {
            result.push_back(a.StringValue());
        }
        return result;
    };

    auto processSettings = [](const TCoNameValueTupleList& settingsList) {
        TVector<TExprNode::TPtr> result;
        for (auto s : settingsList) {
            result.push_back(s.Ptr());
        }
        return result;
    };

    auto columns = opTableEffect.Columns().Maybe<TCoAtomList>();
    auto onConflict = opTableEffect.OnConflict().Maybe<TCoAtom>();
    auto returningColumns = opTableEffect.ReturningColumns().Maybe<TCoAtomList>();
    auto settings = opTableEffect.Settings().Maybe<TCoNameValueTupleList>();
    auto isBatch = opTableEffect.IsBatch().Maybe<TCoAtom>();

    PlanProps.WithReturning = (bool)returningColumns;

    if (effectType == "TKqlInsertRows") {
        type = EEffectType::InsertRows;

        options.Columns = processColumns(columns.Cast());
        options.OnConflict = onConflict.Cast().StringValue();
        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.Settings = processSettings(settings.Cast());
    } else if (effectType == "TKqlInsertRowsIndex") {
        type = EEffectType::InsertRowsIndex;

        options.Columns = processColumns(columns.Cast());
        options.OnConflict = onConflict.Cast().StringValue();
        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.IsBatch = isBatch.Cast().StringValue() == "true";
        options.Settings = processSettings(settings.Cast());
    } else if (effectType == "TKqlUpdateRows") {
        type = EEffectType::UpdateRows;

        options.Columns = processColumns(columns.Cast());
        options.ReturningColumns = processColumns(returningColumns.Cast());
    } else if (effectType == "TKqlUpdateRowsIndex") {
        type = EEffectType::UpdateRowsIndex;

        options.Columns = processColumns(columns.Cast());
        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.IsBatch = isBatch.Cast().StringValue() == "true";
        options.Settings = processSettings(settings.Cast());
    } else if (effectType == "KqlUpsertRows") {
        type = EEffectType::UpsertRows;

        options.Columns = processColumns(columns.Cast());
        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.IsBatch = isBatch.Cast().StringValue() == "true";
        if (auto defaultColumns = opTableEffect.DefaultColumns().Maybe<TCoAtomList>()) {
            options.DefaultColumns = processColumns(defaultColumns.Cast());
        }
        if (settings) {
            options.Settings = processSettings(settings.Cast());
        }
    } else if (effectType == "KqlDeleteRows") {
        type = EEffectType::DeleteRows;

        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.IsBatch = isBatch.Cast().StringValue() == "true";
        options.Settings = processSettings(settings.Cast());
    } else if (effectType == "KqlDeleteRowsIndex") {
        type = EEffectType::DeleteRowsIndex;

        options.ReturningColumns = processColumns(returningColumns.Cast());
        options.IsBatch = isBatch.Cast().StringValue() == "true";
        options.Settings = processSettings(settings.Cast());
    } else {
        Y_ENSURE(false, "Unexpected table effects operation");
    }

    const auto bindings = GetBindings(opTableEffect.Input().Ptr());
    TOrderedIUs<TString> inputColumns;
    const auto* schema = opTableEffect.Input().Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    for (const auto* field : schema->GetItems()) {
        inputColumns.Append(ResolveBinding(*bindings, field->GetName()), TString(field->GetName()));
    }
    auto output = std::make_shared<TExpression::TBindings>();
    TOrderedIUs<TString> returning;
    if (options.ReturningColumns) {
        for (const auto& column : *options.ReturningColumns) {
            const auto id = PlanProps.InfoUnitRegistry.Add(TInfoUnit("", column));
            returning.Append(id, column);
            Y_ENSURE(output->emplace(column, id).second, "Duplicate returning column " << column);
        }
    }
    OutputBindings[TImportKey{node.Get(), BindingContext}] = std::move(output);
    return MakeIntrusive<TOpTableEffect>(std::move(input), node->Pos(), opTableEffect.Table().Ptr(), type, std::move(options),
        std::move(inputColumns), std::move(returning));
}

} // namespace NKikimr::Nkqp
