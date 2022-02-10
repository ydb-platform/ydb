#include "kqp_prepare_impl.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

template<typename TMapNode>
TExprBase RebuildMapToList(TMapNode map, TExprContext& ctx) {
    if (map.Lambda().Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List &&
        map.Input().Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List)
    {
        return map;
    }

    bool isOptional = map.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional;

    if (map.Lambda().Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
        auto newBody = Build<TCoToList>(ctx, map.Pos())
            .Optional(map.Lambda().Body())
            .Done();

        map = Build<TMapNode>(ctx, map.Pos())
            .Input(map.Input())
            .Lambda()
                .Args({"item"})
                .template Body<TExprApplier>()
                    .Apply(newBody)
                    .With(map.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done();
    }

    if (map.Input().Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
        map = Build<TMapNode>(ctx, map.Pos())
            .template Input<TCoToList>()
                .Optional(map.Input())
                .Build()
            .Lambda(map.Lambda())
            .Done();
    }

    if (isOptional) {
        return Build<TCoToOptional>(ctx, map.Pos())
            .List(map)
            .Done();
    }

    YQL_CLOG(INFO, ProviderKqp) << "RebuildMapToList";
    return map;
}

TExprNode::TPtr NormalizeCallables(TExprBase node, TExprContext& ctx, const TKqpAnalyzeResults& analyzeResults) {
    if (!analyzeResults.CallableToExecRootsMap.contains(node.Raw())) {
        return node.Ptr();
    }

    if (node.Maybe<TCoMap>() || node.Maybe<TCoFlatMap>()) {
        return node.Maybe<TCoMap>()
            ? RebuildMapToList<TCoMap>(node.Cast<TCoMap>(), ctx).Ptr()
            : RebuildMapToList<TCoFlatMap>(node.Cast<TCoFlatMap>(), ctx).Ptr();
    }

    if (auto filter = node.Maybe<TCoFilter>()) {
        YQL_CLOG(INFO, ProviderKqp) << "NormalizeCallables: Filter";
        return Build<TCoFlatMap>(ctx, node.Pos())
            .Input(filter.Cast().Input())
            .Lambda()
                .Args({"item"})
                .Body<TCoIf>()
                    .Predicate<TExprApplier>()
                        .Apply(filter.Cast().Lambda())
                        .With(0, "item")
                        .Build()
                    .ThenValue<TCoJust>()
                        .Input("item")
                        .Build()
                    .ElseValue<TCoNothing>()
                        .OptionalType<TCoOptionalType>()
                            .ItemType<TCoTypeOf>()
                                .Value("item")
                                .Build()
                            .Build()
                        .Build()
                .Build()
            .Build()
            .Done()
            .Ptr();
    }

    return node.Ptr();
}

TExprNode::TPtr ToListOverToOptional(TExprBase node, TExprContext& ctx) {
    Y_UNUSED(ctx);

    if (auto toList = node.Maybe<TCoToList>()) {
        if (auto toOpt = toList.Cast().Optional().Maybe<TCoToOptional>()) {
            YQL_CLOG(INFO, ProviderKqp) << "ToListOverToOptional";
            return toOpt.Cast().List().Ptr();
        }
        if (auto toOpt = toList.Cast().Optional().Maybe<TCoHead>()) {
            YQL_CLOG(INFO, ProviderKqp) << "ToListOverHead";
            return toOpt.Cast().Input().Ptr();
        }
        if (auto toOpt = toList.Cast().Optional().Maybe<TCoLast>()) {
            YQL_CLOG(INFO, ProviderKqp) << "ToListOverLast";
            return toOpt.Cast().Input().Ptr();
        }
    }

    return node.Ptr();
}

template<typename TInner, typename TOuter>
TExprBase SplitMap(TExprBase input, TCoLambda lambda, TExprContext& ctx, const TVector<TExprBase> execRoots,
    const TKqpAnalyzeResults& analyzeResults)
{
    auto exprRootsTuple = Build<TExprList>(ctx, lambda.Pos())
        .Add(execRoots)
        .Done();

    auto isSameScope = [lambda] (const TNodeInfo* nodeInfo) {
        if (!nodeInfo) {
            return true;
        }

        if (!nodeInfo->Scope || nodeInfo->Scope->Lambda.Raw() != lambda.Raw()) {
            return false;
        }

        return true;
    };

    THashSet<const TExprNode*> innerNodes;
    innerNodes.insert(lambda.Args().Arg(0).Raw());
    VisitExpr(exprRootsTuple.Ptr(),
        [&innerNodes, &isSameScope, &analyzeResults] (const TExprNode::TPtr& node) {
            auto* nodeInfo = analyzeResults.ExprToNodeInfoMap.FindPtr(node.Get());

            if (!isSameScope(nodeInfo)) {
                return false;
            }

            if (node->IsCallable()) {
                innerNodes.insert(node.Get());
            }

            return true;
        });

    THashMap<const TExprNode*, TExprBase> jointsMap;
    for (TExprBase root : execRoots) {
        jointsMap.insert(std::make_pair(root.Raw(), root));
    }

    VisitExpr(lambda.Body().Ptr(),
        [&innerNodes, &jointsMap, isSameScope, &analyzeResults] (const TExprNode::TPtr& node) {
            auto* nodeInfo = analyzeResults.ExprToNodeInfoMap.FindPtr(node.Get());
            YQL_ENSURE(nodeInfo);

            YQL_ENSURE(node->GetTypeAnn());
            bool isComputable = node->IsComputable();

            if (innerNodes.contains(node.Get())) {
                if (isComputable) {
                    jointsMap.insert(std::make_pair(node.Get(), TExprBase(node)));
                }

                return false;
            }

            bool hasInnerChild = false;
            if (isComputable && !node->Children().empty() && nodeInfo->IsExecutable) {
                for (const auto& child : node->Children()) {
                    if (jointsMap.contains(child.Get())) {
                        return true;
                    }

                    bool innerChild = innerNodes.contains(child.Get());
                    hasInnerChild = hasInnerChild | innerChild;

                    YQL_ENSURE(child->GetTypeAnn());
                    if (child->GetTypeAnn()->IsComputable() && !innerChild) {
                        return true;
                    }
                }

                if (hasInnerChild) {
                    jointsMap.insert(std::make_pair(node.Get(), TExprBase(node)));
                    return false;
                }
            }

            return true;
        });

    TVector<TExprBase> jointNodes;
    jointNodes.reserve(jointsMap.size());
    for (auto& pair : jointsMap) {
        jointNodes.push_back(pair.second);
    }

    auto innerLambdaBody = Build<TExprList>(ctx, lambda.Pos())
        .Add(jointNodes)
        .Done();

    auto innerLambda = Build<TCoLambda>(ctx, lambda.Pos())
        .Args({"item"})
        .Body<TExprApplier>()
            .Apply(innerLambdaBody)
            .With(lambda.Args().Arg(0), "item")
            .Build()
        .Done();

    TCoArgument outerLambdaArg = Build<TCoArgument>(ctx, lambda.Pos())
        .Name("item")
        .Done();

    TNodeOnNodeOwnedMap replaceMap;
    for (size_t i = 0; i < jointNodes.size(); ++i) {
        auto node = Build<TCoNth>(ctx, lambda.Pos())
            .Tuple(outerLambdaArg)
            .Index().Build(i)
            .Done();

        replaceMap.emplace(jointNodes[i].Raw(), node.Ptr());
    }

    auto outerLambdaBody = ctx.ReplaceNodes(lambda.Body().Ptr(), replaceMap);
    auto outerLambda = Build<TCoLambda>(ctx, lambda.Pos())
        .Args({outerLambdaArg})
        .Body(TExprBase(outerLambdaBody))
        .Done();

    return Build<TOuter>(ctx, input.Pos())
        .template Input<TInner>()
            .Input(input)
            .Lambda(innerLambda)
            .Build()
        .Lambda(outerLambda)
        .Done();
}

TExprNode::TPtr SplitMap(TExprBase mapNode, TExprContext& ctx, const TVector<TExprBase>& execRoots,
    const TKqpAnalyzeResults& analyzeResults)
{
    YQL_ENSURE(mapNode.Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);

    if (auto map = mapNode.Maybe<TCoMap>()) {
        YQL_CLOG(INFO, ProviderKqp) << "SplitMap: Map, MapParameter";
        return SplitMap<TCoMap, TKiMapParameter>(map.Cast().Input(), map.Cast().Lambda(), ctx, execRoots,
            analyzeResults).Ptr();
    }

    if (auto map = mapNode.Maybe<TCoFlatMap>()) {
        YQL_CLOG(INFO, ProviderKqp) << "SplitMap: Map, FlatMapParameter";
        YQL_ENSURE(map.Cast().Lambda().Ptr()->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);
        return SplitMap<TCoMap, TKiFlatMapParameter>(map.Cast().Input(), map.Cast().Lambda(), ctx, execRoots,
            analyzeResults).Ptr();
    }

    if (auto map = mapNode.Maybe<TKiMapParameter>()) {
        YQL_CLOG(INFO, ProviderKqp) << "SplitMap: MapParameter, MapParameter";
        return SplitMap<TKiMapParameter, TKiMapParameter>(map.Cast().Input(), map.Cast().Lambda(),
            ctx, execRoots, analyzeResults).Ptr();
    }

    if (auto map = mapNode.Maybe<TKiFlatMapParameter>()) {
        YQL_CLOG(INFO, ProviderKqp) << "SplitMap: MapParameter, FlatMapParameter";
        return SplitMap<TKiMapParameter, TKiFlatMapParameter>(map.Cast().Input(), map.Cast().Lambda(),
            ctx, execRoots, analyzeResults).Ptr();
    }

    YQL_ENSURE(false);
    return nullptr;
}

TExprNode::TPtr UnnestExecutionRoots(TExprBase node, TExprContext& ctx, const TKqpAnalyzeResults& analyzeResults) {
    auto execRoots = analyzeResults.CallableToExecRootsMap.FindPtr(node.Raw());

    if (!execRoots) {
        return node.Ptr();
    }

    if (auto maybeMap = node.Maybe<TCoMap>()) {
        auto map = maybeMap.Cast();
        if (map.Input().Maybe<TCoParameter>()) {
            YQL_CLOG(INFO, ProviderKqp) << "UnnestExecutionRoots: Map parameter";
            return Build<TKiMapParameter>(ctx, node.Pos())
                .Input(map.Input())
                .Lambda(map.Lambda())
                .Done()
                .Ptr();
        }
    }

    if (auto maybeFlatMap = node.Maybe<TCoFlatMap>()) {
        auto flatMap = maybeFlatMap.Cast();
        if (flatMap.Input().Maybe<TCoParameter>()) {
            YQL_CLOG(INFO, ProviderKqp) << "UnnestExecutionRoots: FlatMap parameter";
            return Build<TKiFlatMapParameter>(ctx, node.Pos())
                .Input(flatMap.Input())
                .Lambda(flatMap.Lambda())
                .Done()
                .Ptr();
        }
    }

    if (node.Maybe<TCoMap>() ||
        node.Maybe<TCoFlatMap>() ||
        node.Maybe<TKiMapParameter>() ||
        node.Maybe<TKiFlatMapParameter>())
    {
        return SplitMap(node, ctx, *execRoots, analyzeResults);
    }

    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
        << "Can't rewrite callable for KQP execution: " << node.Ptr()->Content()));
    YQL_ENSURE(false);
    return nullptr;
}

class TKqpRewriteTransformer : public TSyncTransformerBase {
public:
    TKqpRewriteTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx)
        : TransformCtx(transformCtx) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        const auto& analyzeResults = TransformCtx->AnalyzeResults;

        if (analyzeResults.CallableToExecRootsMap.empty()) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = true;
        TStatus status = TStatus::Error;

        status = OptimizeExpr(input, output,
            [&analyzeResults](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto ret = input;
                TExprBase node(input);

                ret = NormalizeCallables(node, ctx, analyzeResults);
                if (ret != input) {
                    return ret;
                }

                ret = ToListOverToOptional(node, ctx);
                if (ret != input) {
                    return ret;
                }

                return ret;
            }, ctx, optSettings);
        YQL_ENSURE(status == TStatus::Ok);

        if (input != output) {
            return TStatus(TStatus::Repeat, true);
        }

        status = OptimizeExpr(input, output,
            [&analyzeResults](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto ret = input;
                TExprBase node(input);

                ret = UnnestExecutionRoots(node, ctx, analyzeResults);
                if (ret != input) {
                    return ret;
                }

                return ret;
            }, ctx, optSettings);
        YQL_ENSURE(status == TStatus::Ok);

        YQL_ENSURE(input != output);
        return TStatus(TStatus::Repeat, true);
    }

private:
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpRewriteTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx) {
    return new TKqpRewriteTransformer(transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
