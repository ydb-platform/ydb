#include "kqp_prepare_impl.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/queue.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

const THashSet<TStringBuf> SafeCallables {
    TCoJust::CallableName(),
    TCoCoalesce::CallableName(),
    TCoToOptional::CallableName(),
    TCoHead::CallableName(), 
    TCoLast::CallableName(), 
    TCoToList::CallableName(),

    TCoMember::CallableName(),
    TCoAsStruct::CallableName(),

    TCoNothing::CallableName(),
    TCoNull::CallableName(),
    TCoDefault::CallableName(),
    TCoExists::CallableName(),

    TCoNth::CallableName()
};

const THashSet<TStringBuf> ModerateCallables {
    TCoAddMember::CallableName(),
    TCoReplaceMember::CallableName(),
};

bool IsSafePayloadCallable(const TCallable& callable) {
    if (callable.Maybe<TCoDataCtor>()) {
        return true;
    }

    if (callable.Maybe<TCoCompare>()) {
        return true;
    }

    if (callable.Maybe<TCoAnd>()) { 
        return true;
    }

    if (callable.Maybe<TCoOr>()) { 
        return true; 
    } 
 
    if (callable.Maybe<TCoBinaryArithmetic>()) {
        return true;
    }

    if (callable.Maybe<TCoCountBase>()) {
        return true;
    }

    auto isOptInput = [](TExprBase input) {
        return input.Maybe<TCoToList>() || input.Maybe<TCoTake>().Input().Maybe<TCoToList>();
    };

    if (auto maybeFilter = callable.Maybe<TCoFilterBase>()) {
        auto filter = maybeFilter.Cast();

        if (filter.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            return true;
        }

        if (isOptInput(filter.Input())) {
            return true;
        }

        return false;
    }

    if (auto maybeMap = callable.Maybe<TCoMapBase>()) {
        auto map = maybeMap.Cast();

        if (map.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional) {
            return true;
        }

        if (isOptInput(map.Input())) {
            if (maybeMap.Maybe<TCoMap>()) {
                return true;
            } else {
                auto body = map.Lambda().Body();

                return body.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional ||
                    body.Maybe<TCoToList>() || body.Maybe<TCoAsList>();
            }
        }

        return false;
    }

    return SafeCallables.contains(callable.CallableName());
}

bool IsModeratePayloadCallable(TCoNameValueTupleList key, const TCallable& callable) {
    if (IsSafePayloadCallable(callable)) {
        return true;
    }

    if (auto selectRow = callable.Maybe<TKiSelectRow>()) {
        return selectRow.Cast().Key().Raw() == key.Raw();
    }

    return ModerateCallables.contains(callable.CallableName());
}

struct TAnalyzeTxContext {
    TExprToNodeInfoMap NodesInfo;
    TVector<TScopedNode> ExecutionRoots;
};

void GatherNodeScopes(TExprBase root, TAnalyzeTxContext& analyzeCtx) {
    TNodeSet visitedNodes;

    TQueue<TMaybe<TExprScope>> scopesQueue;
    scopesQueue.push(TMaybe<TExprScope>());

    while (!scopesQueue.empty()) {
        auto scope = scopesQueue.front();
        scopesQueue.pop();

        auto scopeRoot = scope
            ? scope->Lambda.Body()
            : root;

        VisitExpr(scopeRoot.Ptr(), [scope, &scopesQueue, &analyzeCtx] (const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            if (node.Maybe<TCoLambda>()) {
                return false;
            }

            if (auto callable = node.Maybe<TCallable>()) {
                auto scopeDepth = scope ? scope->Depth : 0;

                for (const auto& arg : node.Cast<TVarArgCallable<TExprBase>>()) {
                    if (auto lambda = arg.Maybe<TCoLambda>()) {
                        TExprScope newScope(callable.Cast(), lambda.Cast(), scopeDepth + 1);
                        scopesQueue.push(newScope);

                        for (const auto& lambdaArg : lambda.Cast().Args()) {
                            analyzeCtx.NodesInfo[lambdaArg.Raw()].Scope = newScope;
                        }
                    }
                }
            }

            return true;
        }, visitedNodes);
    };


    VisitExpr(root.Ptr(),
        [] (const TExprNode::TPtr& exprNode) {
            Y_UNUSED(exprNode);
            return true;
        },
        [&analyzeCtx] (const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            auto& nodeInfo = analyzeCtx.NodesInfo[node.Raw()];
            if (!node.Maybe<TCoArgument>()) {
                YQL_ENSURE(!nodeInfo.Scope);
            }

            TMaybe<TExprScope> scope = nodeInfo.Scope;
            for (const auto& child : exprNode->Children()) {
                if (TMaybeNode<TCoLambda>(child)) {
                    continue;
                }

                auto childScope = analyzeCtx.NodesInfo[child.Get()].Scope;
                if (childScope) {
                    if (scope) {
                        scope = childScope->Depth > scope->Depth
                            ? childScope
                            : scope;
                    } else {
                        scope = childScope;
                    }
                }
            }

            analyzeCtx.NodesInfo[node.Raw()].Scope = scope;
            return true;
        });
}

void RequireImmediate(TExprBase node, TAnalyzeTxContext& ctx) {
    ctx.NodesInfo[node.Raw()].RequireImmediate = true;
}

void RequireImmediateKey(TCoNameValueTupleList key, TAnalyzeTxContext& ctx) {
    for (auto tuple : key) {
        YQL_ENSURE(tuple.Value().IsValid());
        RequireImmediate(tuple.Value().Cast(), ctx);
    }
}

void RequireImmediateRange(TExprList range, TAnalyzeTxContext& ctx) {
    for (auto tuple : range) {
        if (auto columnRange = tuple.Maybe<TKiColumnRangeTuple>()) {
            RequireImmediate(columnRange.Cast().From(), ctx);
            RequireImmediate(columnRange.Cast().To(), ctx);
        }
    }
}

void RequireImmediateSettings(TCoNameValueTupleList settings, TAnalyzeTxContext& ctx) {
    for (auto setting : settings) {
        if (setting.Value() && setting.Value().Ref().IsComputable()) {
            RequireImmediate(setting.Value().Cast(), ctx);
        }
    }
}

void RequireEffectPayloadSafety(TCoNameValueTupleList key, TCoNameValueTupleList payload, TAnalyzeTxContext& ctx,
    ECommitSafety commitSafety)
{
    switch (commitSafety) {
        case ECommitSafety::Full:
            for (auto tuple : payload) {
                YQL_ENSURE(tuple.Value().IsValid());
                RequireImmediate(tuple.Value().Cast(), ctx);
            }
            return;

        case ECommitSafety::Safe:
        case ECommitSafety::Moderate:
            break;

        default:
            YQL_ENSURE(false, "Unexpected commit safety level.");
    }

    VisitExpr(payload.Ptr(), [&ctx, key, commitSafety] (const TExprNode::TPtr& exprNode) {
        TExprBase node(exprNode);

        if (node.Maybe<TCoLambda>()) {
            return false;
        }

        if (node.Maybe<TCoArgument>()) {
            return false;
        }

        if (!node.Ref().IsComputable()) {
            return true;
        }

        if (ctx.NodesInfo[node.Raw()].IsImmediate) {
            return false;
        }

        if (auto maybeList = node.Maybe<TExprList>()) {
            return true;
        }

        if (auto maybeCallable = node.Maybe<TCallable>()) {
            auto callable = maybeCallable.Cast();

            bool safeCallable = commitSafety == ECommitSafety::Safe
                ? IsSafePayloadCallable(callable)
                : IsModeratePayloadCallable(key, callable);

            if (!safeCallable) {
                RequireImmediate(callable, ctx);
                return false;
            }

            for (const auto& arg : callable.Cast<TVarArgCallable<TExprBase>>()) {
                if (auto lambda = arg.Maybe<TCoLambda>()) {
                    auto badNode = FindNode(lambda.Cast().Body().Ptr(),
                        [key, commitSafety] (const TExprNode::TPtr& node) {
                            if (!node->IsCallable()) {
                                return false;
                            }

                            auto callable = TCallable(node);
                            bool safeCallable = commitSafety == ECommitSafety::Safe
                                ? IsSafePayloadCallable(callable)
                                : IsModeratePayloadCallable(key, callable);

                            return !safeCallable;
                        });

                    if (badNode) {
                        RequireImmediate(callable, ctx);
                        return false;
                    }
                }
            }

            return true;
        }

        RequireImmediate(node, ctx);
        return false;
    });
}

void MarkImmediateNodes(TExprBase node, TAnalyzeTxContext& ctx) {
    if (node.Maybe<TCoDataType>() ||
        node.Maybe<TCoOptionalType>())
    {
        ctx.NodesInfo[node.Raw()].IsImmediate = true;
    }

    if (node.Maybe<TCoDataCtor>() || node.Maybe<TCoVoid>() || node.Maybe<TCoNothing>() || node.Maybe<TCoNull>()) {
        ctx.NodesInfo[node.Raw()].IsImmediate = true;
    }

    if (node.Maybe<TCoParameter>()) {
        ctx.NodesInfo[node.Raw()].IsImmediate = true;
    }

    if (node.Maybe<TKiMapParameter>() || node.Maybe<TKiFlatMapParameter>()) {
        auto mapLambda = node.Maybe<TKiMapParameter>()
            ? node.Cast<TKiMapParameter>().Lambda()
            : node.Cast<TKiFlatMapParameter>().Lambda();

        ctx.NodesInfo[mapLambda.Args().Arg(0).Raw()].IsImmediate = true;
    }
}

void MarkRequireImmediateNodes(TExprBase node, TAnalyzeTxContext& ctx) {
    if (auto selectRow = node.Maybe<TKiSelectRow>()) {
        RequireImmediateKey(selectRow.Cast().Key(), ctx);
    }

    if (auto selectRange = node.Maybe<TKiSelectRange>()) {
        RequireImmediateRange(selectRange.Cast().Range(), ctx);
        RequireImmediateSettings(selectRange.Cast().Settings(), ctx);
    }

    if (auto updateRow = node.Maybe<TKiUpdateRow>()) {
        RequireImmediateKey(updateRow.Cast().Key(), ctx);
    }

    if (auto eraseRow = node.Maybe<TKiEraseRow>()) {
        RequireImmediateKey(eraseRow.Cast().Key(), ctx);
    }

    if (node.Maybe<TKiMapParameter>() || node.Maybe<TKiFlatMapParameter>()) {
        TExprBase input = node.Maybe<TKiMapParameter>()
            ? node.Cast<TKiMapParameter>().Input()
            : node.Cast<TKiFlatMapParameter>().Input();

        ctx.NodesInfo[input.Raw()].RequireImmediate = true;
    }

    if (auto condEffect = node.Maybe<TKiConditionalEffect>()) {
        ctx.NodesInfo[condEffect.Cast().Predicate().Raw()].RequireImmediate = true;
    }
}

void PropagateImmediateNodes(TExprBase node, TAnalyzeTxContext& ctx) {
    if (auto just = node.Maybe<TCoJust>()) {
        if (ctx.NodesInfo[just.Cast().Input().Raw()].IsImmediate) {
            ctx.NodesInfo[node.Raw()].IsImmediate = true;
        }
    }

    if (auto nth = node.Maybe<TCoNth>()) {
        if (ctx.NodesInfo[nth.Cast().Tuple().Raw()].IsImmediate) {
            ctx.NodesInfo[node.Raw()].IsImmediate = true;
        }
    }

    if (auto member = node.Maybe<TCoMember>()) {
        if (ctx.NodesInfo[member.Cast().Struct().Raw()].IsImmediate) {
            ctx.NodesInfo[node.Raw()].IsImmediate = true;
        }
    }
}

void EnsureNodesSafety(TExprBase node, TAnalyzeTxContext& ctx, ECommitSafety commitSafety, bool topLevel) {
    if (auto updateRow = node.Maybe<TKiUpdateRow>()) {
        RequireEffectPayloadSafety(updateRow.Cast().Key(), updateRow.Cast().Update(), ctx,
            topLevel ? commitSafety : ECommitSafety::Full);
    }
}

bool EnsureEffectsSafety(TExprBase node, TAnalyzeTxContext& ctx) {
    bool hasNewRequirements = false;

    VisitExpr(node.Ptr(), [&ctx, &hasNewRequirements] (const TExprNode::TPtr& exprNode) {
        auto node = TExprBase(exprNode);
        auto& nodeInfo = ctx.NodesInfo[node.Raw()];

        if (nodeInfo.IsImmediate || nodeInfo.RequireImmediate) {
            return false;
        }

        if (node.Maybe<TKiUpdateRow>() || node.Maybe<TKiEraseRow>()) {
            return false;
        }

        if (node.Maybe<TKiSelectRow>() || node.Maybe<TKiSelectRange>()) {
            hasNewRequirements = true;
            RequireImmediate(node, ctx);
            return false;
        }

        return true;
    });

    return !hasNewRequirements;
}

void AnalyzeNode(TExprBase node, TAnalyzeTxContext& ctx) {
    auto& nodeInfo = *ctx.NodesInfo.FindPtr(node.Raw());

    nodeInfo.IsExecutable = true;
    nodeInfo.AreInputsExecutable = true;

    for (const auto& child : node.Ptr()->Children()) {
        const auto& childInfo = *ctx.NodesInfo.FindPtr(child.Get());

        bool canExecute = childInfo.IsExecutable;
        if (canExecute) {
            if (childInfo.RequireImmediate && !childInfo.IsImmediate) {
                ctx.ExecutionRoots.emplace_back(TExprBase(child), childInfo.Scope);
                canExecute = false;
            }
        }

        nodeInfo.IsExecutable = nodeInfo.IsExecutable && canExecute;

        if (!TMaybeNode<TCoLambda>(child)) {
            nodeInfo.AreInputsExecutable = nodeInfo.AreInputsExecutable && canExecute;
        }

        if (!nodeInfo.IsExecutable && !nodeInfo.AreInputsExecutable) {
            break;
        }
    }
}

void AnalyzeNodes(const TVector<TExprBase>& nodes, TAnalyzeTxContext& ctx) {
    YQL_ENSURE(ctx.ExecutionRoots.empty());

    for (auto& node : nodes) {
        AnalyzeNode(node, ctx);
    }
}

bool Analyze(const TExprNode::TPtr& exprRoot, TKqpAnalyzeResults& results, ECommitSafety commitSafety) {
    TAnalyzeTxContext analyzeCtx;
    TVector<TExprBase> nodes;

    GatherNodeScopes(TExprBase(exprRoot), analyzeCtx);

    VisitExpr(exprRoot,
        [&analyzeCtx] (const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            MarkImmediateNodes(node, analyzeCtx);
            MarkRequireImmediateNodes(node, analyzeCtx);
            return true;
        },
        [&analyzeCtx, &nodes, commitSafety] (const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);
            auto scope = analyzeCtx.NodesInfo[exprNode.Get()].Scope;

            PropagateImmediateNodes(node, analyzeCtx);
            EnsureNodesSafety(node, analyzeCtx, commitSafety, !scope);

            nodes.push_back(node);
            return true;
        });

    AnalyzeNodes(nodes, analyzeCtx);

    auto& rootInfo = analyzeCtx.NodesInfo[exprRoot.Get()];

    if (rootInfo.IsExecutable) {
        if (!EnsureEffectsSafety(TKiProgram(exprRoot).Effects(), analyzeCtx)) {
            AnalyzeNodes(nodes, analyzeCtx);
        }
    }

    results.CanExecute = rootInfo.IsExecutable;
    results.ExprToNodeInfoMap = std::move(analyzeCtx.NodesInfo);

    results.ExecutionRoots.clear();
    results.CallableToExecRootsMap.clear();
    results.LambdaToExecRootsMap.clear();
    for (auto& execRoot : analyzeCtx.ExecutionRoots) {
        if (execRoot.Scope) {
            auto callableInfo = results.ExprToNodeInfoMap.FindPtr(execRoot.Scope->Callable.Raw());
            YQL_ENSURE(callableInfo);

            if (!callableInfo->AreInputsExecutable) {
                continue;
            }

            results.LambdaToExecRootsMap[execRoot.Scope->Lambda.Raw()].push_back(execRoot.Node);
            results.CallableToExecRootsMap[execRoot.Scope->Callable.Raw()].push_back(execRoot.Node);
        }

        results.ExecutionRoots.push_back(execRoot);
    }

    YQL_ENSURE(results.CanExecute || !results.ExecutionRoots.empty());

    return true;
}

class TKqpAnalyzeTransformer : public TSyncTransformerBase {
public:
    TKqpAnalyzeTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx)
        : TransformCtx(transformCtx) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        Y_UNUSED(ctx);
        output = input; 

        auto commitSafety = TransformCtx->CommitSafety();
        if (!TransformCtx->Config->HasAllowKqpUnsafeCommit()) {
            switch (commitSafety) {
                case ECommitSafety::Full:
                case ECommitSafety::Safe:
                    break;
                default:
                    ctx.AddError(YqlIssue(ctx.GetPosition(input->Pos()), TIssuesIds::KIKIMR_BAD_OPERATION,
                        "Unsafe commits not allowed for current database."));
                    return TStatus::Error;
            }
        }

        if (!Analyze(input.Get(), TransformCtx->AnalyzeResults, commitSafety)) {
            return TStatus::Error;
        }

        YQL_CLOG(DEBUG, ProviderKqp) << "Analyze results:" << Endl
            << "CanExecute: " << TransformCtx->AnalyzeResults.CanExecute << Endl
            << "ExecutionRoots: " << TransformCtx->AnalyzeResults.ExecutionRoots.size();
        return TStatus::Ok;
    }

private:
    TIntrusivePtr<TKqlTransformContext> TransformCtx;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpAnalyzeTransformer(TIntrusivePtr<TKqlTransformContext> transformCtx) {
    return new TKqpAnalyzeTransformer(transformCtx);
}

} // namespace NKqp
} // namespace NKikimr
