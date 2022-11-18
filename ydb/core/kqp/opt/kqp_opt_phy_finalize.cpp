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

TStatus KqpBuildPureExprStagesResult(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    output = input;

    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();

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

    TNodeOnNodeOwnedMap replaces;
    for (const auto& block : query.Blocks()) {

        if (block.Results().Empty()) {
            continue;
        }

        // Currently we compute all query results in the last physical transaction, so we cannot
        // precompute them early if no explicit DqPhyPrecompute is specified.
        // Removing precompute in queries with multiple physical tx can lead to additional computations.
        // More proper way to fix this is to allow partial result computation in early physical
        // transactions.
        bool omitResultPrecomputes = true;
        for (const auto& result : block.Results()) {
            if (result.Value().Maybe<TDqPhyPrecompute>()) {
                continue;
            }

            if (!hasPrecomputes(result.Value())) {
                omitResultPrecomputes = false;
                break;
            }
        }
        for (const auto& effect: block.Effects()) {
            if (!hasPrecomputes(effect)) {
                omitResultPrecomputes = false;
                break;
            }
        }

        for (const auto& queryResult : block.Results()) {
            TExprBase node(queryResult.Value());

            // TODO: Missing support for DqCnValue results in scan queries
            if (node.Maybe<TDqPhyPrecompute>() && omitResultPrecomputes && !kqpCtx.IsScanQuery()) {
                replaces[node.Raw()] = node.Cast<TDqPhyPrecompute>().Connection().Ptr();
            } else {
                auto result = DqBuildPureExprStage(node, ctx);
                if (result.Raw() != node.Raw()) {
                    YQL_CLOG(DEBUG, ProviderKqp) << "Building stage out of pure query #" << node.Raw()->UniqueId();
                    replaces[node.Raw()] = result.Ptr();
                }
            }
        }
    }
    output = ctx.ReplaceNodes(TExprNode::TPtr(input), replaces);

    return TStatus::Ok;
}

TStatus KqpBuildUnionResult(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();
    TNodeOnNodeOwnedMap replaces;

    for (const auto& block : query.Blocks()) {
        for (const auto& queryResult: block.Results()) {
            TExprBase node(queryResult.Value());

            auto result = DqBuildExtendStage(node, ctx);
            if (result.Raw() != node.Raw()) {
                YQL_CLOG(DEBUG, ProviderKqp) << "Building stage out of union #" << node.Raw()->UniqueId();
                replaces[node.Raw()] = result.Ptr();
            }
        }
    }
    output = ctx.ReplaceNodes(TExprNode::TPtr(input), replaces);

    return TStatus::Ok;
}

TStatus KqpDuplicateResults(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;

    TExprBase inputExpr(input);
    auto query = inputExpr.Cast<TKqlQuery>();

    struct TKqlQueryResultInfo {
        TMaybeNode<TKqlQueryResult> Node;
        TVector<std::pair<size_t, size_t>> Indexes;
        TMaybeNode<TDqStage> ReplicateStage;
        size_t NextIndex = 0;
    };

    TNodeMap<TKqlQueryResultInfo> kqlQueryResults;
    bool hasDups = false;

    for (size_t blockId = 0; blockId < query.Blocks().Size(); ++blockId) {
        TKqlQueryBlock block = query.Blocks().Item(blockId);
        for (size_t resultId = 0; resultId < block.Results().Size(); ++resultId) {
            TKqlQueryResult result = block.Results().Item(resultId);

            if (!result.Value().Maybe<TDqConnection>()) {
                return TStatus::Ok;
            }

            auto& info = kqlQueryResults[result.Raw()];
            if (info.Indexes.empty()) {
                info.Node = result;
            } else {
                hasDups = true;
            }
            info.Indexes.push_back({blockId, resultId});
        }
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

    TVector<TKqlQueryBlock> queryBlocks;
    queryBlocks.reserve(query.Blocks().Size());
    for (const auto& block : query.Blocks()) {
        TVector<TExprNode::TPtr> results(block.Results().Size());
        for (size_t i = 0; i < block.Results().Size(); ++i) {
            auto& info = kqlQueryResults.at(block.Results().Item(i).Raw());

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

        queryBlocks.emplace_back(Build<TKqlQueryBlock>(ctx, block.Pos())
            .Results()
                .Add(results)
                .Build()
            .Effects(block.Effects())
            .Done());
    }

    output = Build<TKqlQuery>(ctx, query.Pos())
        .Blocks()
            .Add(queryBlocks)
            .Build()
        .Done().Ptr();

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
