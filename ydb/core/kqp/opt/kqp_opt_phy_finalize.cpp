#include "kqp_opt_impl.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy_finalizing.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

TExprBase BuildValueResult(const TDqCnValue& cn, TExprContext& ctx) {
    YQL_ENSURE(cn.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::List);

    return Build<TCoFlatMap>(ctx, cn.Pos())
        .Input<TDqCnUnionAll>()
            .Output(cn.Output())
            .Build()
        .Lambda()
            .Args({"list"})
            .Body("list")
            .Build()
        .Done();
}

TStatus KqpBuildPureExprStagesResult(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    output = input;

    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();

    if (query.Results().Empty()) {
        return TStatus::Ok;
    }

    auto predicate = [](const NYql::TExprNode::TPtr& node) {
        return TMaybeNode<TDqPhyPrecompute>(node).IsValid();
    };

    auto filter = [](const NYql::TExprNode::TPtr& node) {
        return !TMaybeNode<TCoLambda>(node).IsValid();
    };

    auto hasPrecomputes = [&](TExprBase node) {
        auto precompute = FindNode(node.Ptr(), filter, predicate);
        return precompute != nullptr;
    };

    // Currently we compute all query results in the last physical transaction, so we cannot
    // precompute them early if no explicit DqPhyPrecompute is specified.
    // Removing precompute in queries with multiple physical tx can lead to additional computations.
    // More proper way to fix this is to allow partial result computation in early physical
    // transactions.
    bool omitResultPrecomputes = true;
    for (const auto& result : query.Results()) {
        if (result.Value().Maybe<TDqPhyPrecompute>()) {
            continue;
        }

        if (!hasPrecomputes(result.Value())) {
            omitResultPrecomputes = false;
            break;
        }
    }
    for (const auto& effect : query.Effects()) {
        if (!hasPrecomputes(effect)) {
            omitResultPrecomputes = false;
            break;
        }
    }

    TNodeOnNodeOwnedMap replaces;
    for (const auto& queryResult : query.Results()) {
        TExprBase node(queryResult.Value());

        // TODO: Missing support for DqCnValue results in scan queries
        if (node.Maybe<TDqPhyPrecompute>() && omitResultPrecomputes && !kqpCtx.IsScanQuery()) {
            YQL_CLOG(DEBUG, ProviderKqp) << "Building precompute result #" << node.Raw()->UniqueId();

            auto connection = node.Cast<TDqPhyPrecompute>().Connection();
            if (connection.Maybe<TDqCnValue>()) {
                replaces[node.Raw()] = BuildValueResult(connection.Cast<TDqCnValue>(), ctx).Ptr();
            } else {
                YQL_ENSURE(connection.Maybe<TDqCnUnionAll>());
                replaces[node.Raw()] = connection.Ptr();
            }
        } else {
            auto result = DqBuildPureExprStage(node, ctx);
            if (result.Raw() != node.Raw()) {
                YQL_CLOG(DEBUG, ProviderKqp) << "Building stage out of pure query #" << node.Raw()->UniqueId();
                replaces[node.Raw()] = result.Ptr();
            }
        }
    }

    if (replaces.empty()) {
        return TStatus::Ok;
    }

    output = ctx.ReplaceNodes(TExprNode::TPtr(input), replaces);
    return TStatus(TStatus::Repeat, true);
}

TStatus KqpBuildUnionResult(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();
    TNodeOnNodeOwnedMap replaces;

    for (const auto& queryResult : query.Results()) {
        TExprBase node(queryResult.Value());

        auto result = DqBuildExtendStage(node, ctx);
        if (result.Raw() != node.Raw()) {
            YQL_CLOG(DEBUG, ProviderKqp) << "Building stage out of union #" << node.Raw()->UniqueId();
            replaces[node.Raw()] = result.Ptr();
        }
    }
    output = ctx.ReplaceNodes(TExprNode::TPtr(input), replaces);

    return TStatus::Ok;
}

TStatus KqpDuplicateResults(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;

    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();

    if (query.Results().Size() <= 1) {
        return TStatus::Ok;
    }

    struct TKqlQueryResultInfo {
        TMaybeNode<TKqlQueryResult> Node;
        TVector<size_t> Indexes;
        TMaybeNode<TDqStage> ReplicateStage;
        size_t NextIndex = 0;
    };

    TNodeMap<TKqlQueryResultInfo> kqlQueryResults;
    bool hasDups = false;

    for (size_t resultId = 0; resultId < query.Results().Size(); ++resultId) {
        TKqlQueryResult result = query.Results().Item(resultId);

        if (!result.Value().Maybe<TDqConnection>()) {
            return TStatus::Ok;
        }

        auto& info = kqlQueryResults[result.Raw()];
        if (info.Indexes.empty()) {
            info.Node = result;
        } else {
            hasDups = true;
        }
        info.Indexes.push_back(resultId);
    }

    if (!hasDups) {
        return TStatus::Ok;
    }

    auto identityLambda = NDq::BuildIdentityLambda(query.Pos(), ctx);

    for (auto& [ptr, info] : kqlQueryResults) {
        if (info.Indexes.size() > 1) {
            // YQL_CLOG(TRACE, ProviderKqp) << "  * " << KqpExprToPrettyString(TExprBase(ptr), ctx)
            //     << " --> [" << JoinSeq(',', info.Indexes) << "]";

            info.ReplicateStage = Build<TDqStage>(ctx, query.Pos())
                .Inputs()
                    .Add(info.Node.Cast().Value())
                    .Build()
                .Program()
                    .Args({"stream"})
                    .Body<TDqReplicate>()
                        .Input("stream")
                        .FreeArgs()
                            .Add(TVector<TExprBase>(info.Indexes.size(), identityLambda))
                            .Build()
                        .Build()
                    .Build()
                .Settings().Build()
                .Done();
        }
    }

    TVector<TExprNode::TPtr> results(query.Results().Size());
    for (size_t i = 0; i < query.Results().Size(); ++i) {
        auto& info = kqlQueryResults.at(query.Results().Item(i).Raw());

        if (info.Indexes.size() == 1) {
            results[i] = info.Node.Cast().Ptr();
        } else {
            results[i] = Build<TKqlQueryResult>(ctx, query.Pos())
                .Value<TDqCnUnionAll>()
                    .Output()
                        .Stage(info.ReplicateStage.Cast())
                        .Index().Build(ToString(info.NextIndex))
                        .Build()
                    .Build()
                .ColumnHints(info.Node.Cast().ColumnHints())
                .Done().Ptr();
            info.NextIndex++;
        }
    }

    output = Build<TKqlQuery>(ctx, query.Pos())
        .Results()
            .Add(results)
            .Build()
        .Effects(query.Effects())
        .Done().Ptr();

    return TStatus::Ok;
}

template <typename TExpr>
TVector<TExpr> CollectNodes(const TExprNode::TPtr& input) {
    TVector<TExpr> result;

    VisitExpr(input, [&result](const TExprNode::TPtr& node) {
        if (TExpr::Match(node.Get())) {
            result.emplace_back(TExpr(node));
        }
        return true;
    });

    return result;
}

bool FindPrecomputedOutputs(TDqStageBase stage, const TParentsMap& parentsMap) {
    auto outIt = parentsMap.find(stage.Raw());
    if (outIt == parentsMap.end()) {
        return false;
    }

    for (auto& output : outIt->second) {
        if (TDqOutput::Match(output)) {
            auto connIt = parentsMap.find(output);
            if (connIt != parentsMap.end()) {
                for (auto maybeConn : connIt->second) {
                    auto parentIt = parentsMap.find(maybeConn);
                    if (parentIt != parentsMap.end()) {
                        for (auto& parent : parentIt->second) {
                            if (TDqPrecompute::Match(parent) || TDqPhyPrecompute::Match(parent)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }

    return false;
}


TExprBase ReplicatePrecompute(TDqStageBase stage, TExprContext& ctx, const TParentsMap& parentsMap) {
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        auto input = stage.Inputs().Item(i);
        if (auto maybeConn = stage.Inputs().Item(i).Maybe<TDqConnection>()) {
            auto conn = maybeConn.Cast();
            if (conn.Maybe<TDqCnValue>() || conn.Maybe<TDqCnUnionAll>()) {
                {
                    auto sourceStage = conn.Output().Stage();
                    if (!sourceStage.Program().Body().Maybe<TDqReplicate>()) {
                        continue;
                    }

                    if (!FindPrecomputedOutputs(sourceStage, parentsMap)) {
                        continue;
                    }
                }

                auto arg = stage.Program().Args().Arg(i);
                auto newArg = Build<TCoArgument>(ctx, stage.Program().Args().Arg(i).Pos())
                    .Name("_replaced_arg")
                    .Done();

                TVector<TCoArgument> newArgs;
                TNodeOnNodeOwnedMap programReplaces;
                for (size_t j = 0; j < stage.Program().Args().Size(); ++j) {
                    auto oldArg = stage.Program().Args().Arg(j);
                    newArgs.push_back(Build<TCoArgument>(ctx, stage.Program().Args().Arg(i).Pos())
                        .Name("_replaced_arg_" + ToString(j))
                        .Done());
                    if (i == j) {
                        programReplaces[oldArg.Raw()] = Build<TCoToFlow>(ctx, oldArg.Pos()).Input(newArgs.back()).Done().Ptr();
                    } else {
                        programReplaces[oldArg.Raw()] = newArgs.back().Ptr();
                    }
                }

                return
                    Build<TDqStage>(ctx, stage.Pos())
                        .Inputs(ctx.ReplaceNode(stage.Inputs().Ptr(), input.Ref(), Build<TDqPhyPrecompute>(ctx, input.Pos()).Connection(conn).Done().Ptr()))
                        .Outputs(stage.Outputs())
                        .Settings(stage.Settings())
                        .Program()
                            .Args(newArgs)
                            .Body(TExprBase(ctx.ReplaceNodes(stage.Program().Body().Ptr(), programReplaces)))
                            .Build()
                    .Done();
            }
        }
    }
    return stage;
}

NYql::IGraphTransformer::TStatus ReplicatePrecomputeRule(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    TParentsMap parents;
    GatherParents(*input, parents);
    auto stages = CollectNodes<TDqStageBase>(input);
    for (auto& stage : stages) {
        auto applied = ReplicatePrecompute(stage, ctx, parents);
        if (applied.Raw() != stage.Raw()) {
            output = ctx.ReplaceNode(input.Get(), stage.Ref(), applied.Ptr());
            return TStatus::Repeat;
        }
    }
    output = input;
    return TStatus::Ok;
}

template <typename TFunctor>
NYql::IGraphTransformer::TStatus PerformGlobalRule(const TString& ruleName, const NYql::TExprNode::TPtr& input,
    NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx, TFunctor func)
{
    auto status = func(input, output, ctx);

    if (output == input) {
        return status;
    }

    DumpAppliedRule(ruleName, input, output, ctx);

    // restart pipeline
    return NYql::IGraphTransformer::TStatus(NYql::IGraphTransformer::TStatus::Repeat, true);
}

} // anonymous namespace

#define PERFORM_GLOBAL_RULE(id, input, output, ctx, ...) \
    do { \
        auto status = PerformGlobalRule(id, input, output, ctx, __VA_ARGS__); \
        if (status != IGraphTransformer::TStatus::Ok) { return status; } \
    } while (0)

TAutoPtr<IGraphTransformer> CreateKqpFinalizingOptTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx) {
    return CreateFunctorTransformer(
        [kqpCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
            output = input;

            PERFORM_GLOBAL_RULE("ReplicatePrecompute", input, output, ctx, ReplicatePrecomputeRule);

            PERFORM_GLOBAL_RULE("ReplicateMultiUsedConnection", input, output, ctx,
                [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    YQL_ENSURE(TKqlQuery::Match(input.Get()));
                    return NDq::DqReplicateStageMultiOutput(input, output, ctx);
                });

            PERFORM_GLOBAL_RULE("BuildPureExprStages", input, output, ctx,
                [kqpCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    return KqpBuildPureExprStagesResult(input, output, ctx, *kqpCtx);
                });

            PERFORM_GLOBAL_RULE("BuildUnion", input, output, ctx,
                [] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    return KqpBuildUnionResult(input, output, ctx);
                });

            PERFORM_GLOBAL_RULE("ExtractPrecomputeToInputs", input, output, ctx,
                [] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    return DqExtractPrecomputeToStageInput(input, output, ctx);
                });

            PERFORM_GLOBAL_RULE("DuplicateResults", input, output, ctx,
                [] (const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    return KqpDuplicateResults(input, output, ctx);
                });

            YQL_CLOG(TRACE, ProviderKqp) << "FinalizingOptimized KQL query: " << KqpExprToPrettyString(*input, ctx);

            return TStatus::Ok;
        });
}

#undef PERFORM_GLOBAL_RULE

} // namespace NKikimr::NKqp::NOpt
