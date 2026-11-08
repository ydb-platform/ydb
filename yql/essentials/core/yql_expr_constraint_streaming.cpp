#include "yql_expr_constraint_streaming.h"

#include "yql_expr_constraint.h"
#include "yql_expr_optimize.h"

namespace NYql {

using namespace NNodes;

namespace {

bool IsCastCallable(TExprBase node) {
    return node.Raw()->IsCallable({"SafeCast", "StrictCast"});
}

TPartOfConstraintBase::TSetType CollectDirectEventTimePaths(TExprBase body, TExprBase arg) {
    TPartOfConstraintBase::TSetType paths;
    VisitExpr(body.Ref(), [&](const TExprNode& node) {
        if (auto path = TryExtractDirectEventTimePath(TExprBase(&node), arg); path && !path->empty()) {
            paths.insert(*path);
        }
        return true;
    });
    return paths;
}

const TPartOfStreamingConstraintNode* MakeEventTimeLineageProbe(
    const TPartOfConstraintBase::TSetType& paths,
    TExprContext& ctx) {
    YQL_ENSURE(!paths.empty());
    TPartOfStreamingConstraintNode::TMapType mapping;
    for (const auto& path : paths) {
        const auto origin = ctx.MakeConstraint<TStreamingConstraintNode>(path);
        TPartOfStreamingConstraintNode::UniqueMerge(
            mapping,
            TPartOfStreamingConstraintNode::GetCommonMapping(origin));
    }
    return ctx.MakeConstraint<TPartOfStreamingConstraintNode>(std::move(mapping));
}

void CollectEventTimeLineageProbePaths(
    const TTypeAnnotationNode& type,
    TPartOfConstraintBase::TPathType& path,
    TPartOfConstraintBase::TSetType& paths,
    TExprContext& ctx) {
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Optional:
            CollectEventTimeLineageProbePaths(*type.Cast<TOptionalExprType>()->GetItemType(), path, paths, ctx);
            break;
        case ETypeAnnotationKind::Struct:
            for (const auto item : type.Cast<TStructExprType>()->GetItems()) {
                path.emplace_back(item->GetName());
                CollectEventTimeLineageProbePaths(*item->GetItemType(), path, paths, ctx);
                path.pop_back();
            }
            break;
        case ETypeAnnotationKind::Tuple: {
            ui32 index = 0U;
            for (const auto item : type.Cast<TTupleExprType>()->GetItems()) {
                path.emplace_back(ctx.GetIndexAsString(index++));
                CollectEventTimeLineageProbePaths(*item, path, paths, ctx);
                path.pop_back();
            }
            break;
        }
        case ETypeAnnotationKind::Multi: {
            ui32 index = 0U;
            for (const auto item : type.Cast<TMultiExprType>()->GetItems()) {
                path.emplace_back(ctx.GetIndexAsString(index++));
                CollectEventTimeLineageProbePaths(*item, path, paths, ctx);
                path.pop_back();
            }
            break;
        }
        case ETypeAnnotationKind::Variant:
            CollectEventTimeLineageProbePaths(*type.Cast<TVariantExprType>()->GetUnderlyingType(), path, paths, ctx);
            break;
        default:
            paths.insert(path);
            break;
    }
}

} // namespace

TMaybe<TPartOfConstraintBase::TPathType> TryExtractDirectEventTimePath(TExprBase node, TExprBase arg) {
    if (IsCastCallable(node)) {
        node = TExprBase(node.Ref().ChildPtr(0));
    }

    if (node.Raw() == arg.Raw()) {
        return TPartOfConstraintBase::TPathType{};
    }

    const TExprNode* container = nullptr;
    std::string_view name;
    if (const auto maybeMember = node.Maybe<TCoMember>()) {
        const auto member = maybeMember.Cast();
        container = member.Struct().Raw();
        name = member.Name().Value();
    } else if (const auto maybeNth = node.Maybe<TCoNth>()) {
        const auto nth = maybeNth.Cast();
        container = nth.Tuple().Raw();
        name = nth.Index().Value();
    } else {
        return Nothing();
    }

    auto path = TryExtractDirectEventTimePath(TExprBase(container), arg);
    if (!path) {
        return Nothing();
    }

    path->push_back(name);
    return path;
}

TMaybe<TPartOfConstraintBase::TPathType> TryExtractEventTimeFromConstraints(const TExprNode& body) {
    const auto partOfStreaming = body.GetConstraint<TPartOfStreamingConstraintNode>();
    if (!partOfStreaming) {
        return Nothing();
    }

    TMaybe<TPartOfConstraintBase::TPathType> eventTime;
    for (const auto& [streaming, mapping] : partOfStreaming->GetColumnMapping()) {
        if (!streaming->GetEventTime()) {
            continue;
        }

        const auto it = mapping.find(TPartOfConstraintBase::TPathType{});
        if (it == mapping.end() || it->second != *streaming->GetEventTime()) {
            continue;
        }

        if (eventTime && *eventTime != it->second) {
            return Nothing();
        }
        eventTime = it->second;
    }

    return eventTime;
}

TMaybe<TPartOfConstraintBase::TPathType> TryExtractEventTimeFromConstraints(
    const TExprNode& body,
    const TStreamingConstraintNode& streaming) {
    const auto part = body.GetConstraint<TPartOfStreamingConstraintNode>();
    if (!part || !streaming.GetEventTime()) {
        return Nothing();
    }
    const auto origin = part->GetColumnMapping().find(&streaming);
    if (origin == part->GetColumnMapping().end()) {
        return Nothing();
    }
    const auto scalar = origin->second.find(TPartOfConstraintBase::TPathType{});
    return scalar != origin->second.end() && streaming.IsEventTimePath(scalar->second)
               ? TMaybe<TPartOfConstraintBase::TPathType>(scalar->second)
               : Nothing();
}

IGraphTransformer::TStatus UpdateLambdaEventTimeLineage(TExprNode::TPtr& lambda, TExprContext& ctx) {
    YQL_ENSURE(lambda->IsLambda());
    YQL_ENSURE(lambda->Head().ChildrenSize() == 1U);

    const auto argType = lambda->Head().Head().GetTypeAnn();
    YQL_ENSURE(argType);

    TPartOfConstraintBase::TSetType paths;
    TPartOfConstraintBase::TPathType path;
    CollectEventTimeLineageProbePaths(*argType, path, paths, ctx);

    // Treat every leaf as a distinct interned streaming origin. Constraint
    // propagation records the symbolic input-to-output lineage, and the parent
    // later selects the origin matching its actual event-time constraint.
    // Non-timestamp leaves are intentional: an extractor may parse or cast them.
    TConstraintNode::TListType constraints;
    if (!paths.empty()) {
        constraints.emplace_back(MakeEventTimeLineageProbe(paths, ctx));
    }
    return UpdateLambdaConstraints(lambda, ctx, {constraints});
}

IGraphTransformer::TStatus TryExtractEventTime(
    TExprNode::TPtr& node,
    const TExprNode& eventTimeExpr,
    TExprContext& ctx,
    TMaybe<TPartOfConstraintBase::TPathType>& eventTime) {
    const auto maybeLambda = TExprBase(node).Maybe<TCoLambda>();
    YQL_ENSURE(maybeLambda);
    const auto lambda = maybeLambda.Cast();

    const auto args = lambda.Args();
    YQL_ENSURE(args.Size() == 1U);
    const auto arg = args.Arg(0);
    const auto expr = TExprBase(&eventTimeExpr);

    if (auto direct = TryExtractDirectEventTimePath(expr, arg)) {
        if (const auto status = UpdateLambdaConstraints(node, ctx, {TConstraintNode::TListType{}}); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        eventTime = std::move(*direct);
        return IGraphTransformer::TStatus::Ok;
    }

    const auto paths = CollectDirectEventTimePaths(expr, arg);
    if (paths.empty()) {
        eventTime = Nothing();
        return UpdateLambdaConstraints(node, ctx, {TConstraintNode::TListType{}});
    }

    TConstraintNode::TListType constraints{MakeEventTimeLineageProbe(paths, ctx)};
    if (const auto status = UpdateLambdaConstraints(node, ctx, {constraints}); status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    eventTime = TryExtractEventTimeFromConstraints(eventTimeExpr);
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql
