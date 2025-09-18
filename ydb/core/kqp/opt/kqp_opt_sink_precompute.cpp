#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/core/services/yql_out_transformers.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/table_service_config.pb.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

class TKqpSinkPrecomputeTransformer : public TSyncTransformerBase {
public:
    TKqpSinkPrecomputeTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
        : KqpCtx(kqpCtx)
    {
    }

    TStatus DoTransform(TExprNode::TPtr inputExpr, TExprNode::TPtr& outputExpr, TExprContext& ctx) final {
        outputExpr = inputExpr;

        if (!KqpCtx->Config->EnableOltpSink) {
            return TStatus::Ok;
        }

        const auto stagesUsedForPrecomputesAndSinks = FindStagesUsedForPrecomputeAndSinks(outputExpr);
        Y_UNUSED(stagesUsedForPrecomputesAndSinks);

        TNodeOnNodeOwnedMap marked;
        for (const auto& [_, exprNode] : stagesUsedForPrecomputesAndSinks) {
            AFL_ENSURE(exprNode);
            TExprBase node(exprNode);
            const auto stage = node.Cast<TDqStage>();
            if (HasNonDeterministicFunction(stage)) {
                marked.emplace(node.Raw(), node.Ptr());
            }
        }

        if (marked.empty()) {
            return TStatus::Ok;
        }

        // Find all stages that depend on non-deterministic stages
        // that are used for sinks or precomputes.
        {
            auto filter = [](const TExprNode::TPtr& exprNode) {
                return !exprNode->IsLambda();
            };

            TNodeSet visited;
            auto collector = [&](const TExprNode::TPtr& exprNode) {
                if (TDqStage::Match(exprNode.Get())) {
                    bool foundMarked = false;
                    VisitStagesBackwards(exprNode,
                        [&](const TDqStage& stage) mutable -> bool {
                            if (foundMarked) {
                                return false;
                            } else if (marked.contains(stage.Raw())) {
                                foundMarked = true;
                                return false;
                            } else if (!visited.insert(stage.Raw()).second) {
                                return false;
                            }

                            return true;
                        },
                        [&](const TDqStage& stage) mutable {
                            if (foundMarked) {
                                marked.emplace(stage.Raw(), stage.Ptr());
                            }
                        });
                }
                return true;
            };

            VisitExpr(outputExpr, filter, collector);
        }

        TNodeOnNodeOwnedMap replaces;
        // Add precompute before marked stages with sink
        for (const auto& [_, exprNode] : marked) {
            TExprBase node(exprNode);
            const auto stage = node.Cast<TDqStage>();

            if (stage.Outputs()) {
                const auto outputs = stage.Outputs().Cast();
                AFL_ENSURE(outputs.Size() == 1);
                for (const auto& output : outputs) {
                    if (auto maybeSink = output.Maybe<TDqSink>()) {
                        const auto sink = maybeSink.Cast();
                        if (const auto sinkSettings = sink.Settings().Maybe<TKqpTableSinkSettings>()) {
                            const auto executedAsSingleEffect = sinkSettings.Cast().Mode() == "fill_table"
                                || (KqpCtx->Tables->ExistingTable(KqpCtx->Cluster, sinkSettings.Cast().Table().Path()).Metadata->Kind == EKikimrTableKind::Olap);
                            if (!executedAsSingleEffect) {
                                AFL_ENSURE(stage.Inputs().Size() == 1);

                                auto channel = Build<TDqCnUnionAll>(ctx, node.Pos())
                                    .Output()
                                        .Stage<TDqStage>() // no output
                                            .Inputs(stage.Inputs())
                                            .Program(stage.Program())
                                            .Settings(stage.Settings())
                                            .Build()
                                        .Index().Build("0")
                                        .Build()
                                    .Done();

                                auto inputRows = Build<TDqPhyPrecompute>(ctx, node.Pos())
                                    .Connection(channel)
                                    .Done();

                                auto rowArg = Build<TCoArgument>(ctx, node.Pos())
                                    .Name("rowArg")
                                    .Done();

                                replaces[node.Raw()] = Build<TDqStage>(ctx, node.Pos())
                                    .Inputs()
                                        .Add(inputRows)
                                        .Build()
                                    .Program()
                                        .Args({rowArg})
                                        .Body<TCoToFlow>()
                                            .Input(rowArg)
                                            .Build()
                                        .Build()
                                    .Outputs<TDqStageOutputsList>()
                                        .Add(sink)
                                        .Build()
                                    .Settings().Build()
                                    .Done().Ptr();
                            }
                        }
                    }
                }
            }
        }

        outputExpr = ctx.ReplaceNodes(std::move(outputExpr), replaces);

        return TStatus::Ok;
    }

    void Rewind() final {
    }

private:
    TNodeOnNodeOwnedMap FindStagesUsedForPrecomputeAndSinks(TExprNode::TPtr& expr) {
        const auto [precomputeStages, sinkStages] = GatherPrecomputesAndSinks(expr, *KqpCtx);

        TNodeSet stagesUsedForPrecomputes;
        for (const auto& precomputeStage : precomputeStages) {
            auto visit = [&stagesUsedForPrecomputes](const TDqStage& stage) mutable -> bool {
                return stagesUsedForPrecomputes.emplace(stage.Raw()).second;
            };
            auto postVisit = [](const TDqStage&) {};
            VisitStagesBackwards(precomputeStage.second, visit, postVisit);
        }

        TNodeOnNodeOwnedMap stagesUsedForPrecomputesAndSinks;
        TNodeSet stagesUsedForSinks;
        for (const auto& sinkStage : sinkStages) {
            auto visit = [&stagesUsedForSinks, &stagesUsedForPrecomputes, &stagesUsedForPrecomputesAndSinks](
                            const TDqStage& stage) mutable -> bool {
                if (stagesUsedForPrecomputes.contains(stage.Raw())) {
                    stagesUsedForPrecomputesAndSinks.emplace(stage.Raw(), stage.Ptr());
                }
                return stagesUsedForSinks.emplace(stage.Raw()).second;
            };
            auto postVisit = [](const TDqStage&) {};
            VisitStagesBackwards(sinkStage.second, visit, postVisit);
        }

        return stagesUsedForPrecomputesAndSinks;
    }

    std::pair<TNodeOnNodeOwnedMap, TNodeOnNodeOwnedMap> GatherPrecomputesAndSinks(const TExprNode::TPtr& query, const TKqpOptimizeContext& kqpCtx) {
        TNodeOnNodeOwnedMap precomputeStages;
        TNodeOnNodeOwnedMap sinkStages;

        auto filter = [](const TExprNode::TPtr& exprNode) {
            return !exprNode->IsLambda();
        };

        auto gather = [&precomputeStages, &sinkStages, &kqpCtx](const TExprNode::TPtr& exprNode) {
            TExprBase node(exprNode);

            const auto maybeStage = node.Maybe<TDqStage>();

            if (!maybeStage.IsValid()) {
                return true;
            }

            const auto stage = maybeStage.Cast();

            for (const auto& input : stage.Inputs()) {
                if (auto maybePrecompute = input.Maybe<TDqPhyPrecompute>()) {
                    const auto precomputeStage = maybePrecompute.Cast().Connection().Output().Stage();
                    precomputeStages.emplace(precomputeStage.Raw(), precomputeStage.Ptr());
                } else if (auto maybeSource = input.Maybe<TDqSource>()) {
                    VisitExpr(maybeSource.Cast().Ptr(),
                        [&] (const TExprNode::TPtr& ptr) {
                            TExprBase node(ptr);
                            if (auto maybePrecompute = node.Maybe<TDqPhyPrecompute>()) {
                                const auto precomputeStage = maybePrecompute.Cast().Connection().Output().Stage();
                                precomputeStages.emplace(precomputeStage.Raw(), precomputeStage.Ptr());
                                return false;
                            }
                            return true;
                        });
                } else if (input.Maybe<TKqpTxResultBinding>()) {
                    // This transformer must be executed before building transactions 
                    YQL_ENSURE(false, "Unexpected TKqpTxResultBinding");
                }
            }

            if (stage.Outputs()) {
                const auto outputs = stage.Outputs().Cast();
                AFL_ENSURE(outputs.Size() == 1);
                for (const auto& output : outputs) {
                    if (auto maybeSink = output.Maybe<TDqSink>()) {
                        const auto sink = maybeSink.Cast();
                        if (const auto sinkSettings = sink.Settings().Maybe<TKqpTableSinkSettings>()) {
                            const auto executedAsSingleEffect = sinkSettings.Cast().Mode() == "fill_table"
                                || (kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, sinkSettings.Cast().Table().Path()).Metadata->Kind == EKikimrTableKind::Olap);
                            if (!executedAsSingleEffect) {
                                sinkStages.emplace(stage.Raw(), stage.Ptr());
                            }
                        }
                    }
                }
            }

            return true;
        };

        VisitExpr(query, filter, gather);

        return {std::move(precomputeStages), std::move(sinkStages)};
    }

    bool HasNonDeterministicFunction(const TDqStage& stage) {
        bool hasNonDeterministicFunction = false;
        VisitExpr(stage.Program().Ptr(), [&hasNonDeterministicFunction](const TExprNode::TPtr& exprNode) mutable {
            if (auto maybeCallable = TMaybeNode<TCallable>(exprNode)) {
                auto callable = maybeCallable.Cast();
                if (GetNonDeterministicFunctions().contains(callable.CallableName())) {
                    hasNonDeterministicFunction = true;
                }
            }
            return !hasNonDeterministicFunction;
        });
        return hasNonDeterministicFunction;
    }

    void VisitStagesBackwards(
            const TExprNode::TPtr& exprNode,
            std::function<bool(const TDqStage&)> visitor,
            std::function<void(const TDqStage&)> postVisitor) {
        TExprBase node(exprNode);
        const auto maybeStage = node.Maybe<TDqStage>();
        AFL_ENSURE(maybeStage.IsValid());

        const auto stage = maybeStage.Cast();
        if (visitor(stage)) {
            for (const auto& input : stage.Inputs()) {
                if (auto maybeConnection = input.Maybe<TDqConnection>()) {
                    const auto inputStage = maybeConnection.Cast().Output().Stage();
                    VisitStagesBackwards(inputStage.Ptr(), visitor, postVisitor);
                }
            }

            postVisitor(stage);
        }
    }

private:
    TIntrusivePtr<TKqpOptimizeContext> KqpCtx;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpSinkPrecomputeTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx)
{
    return new TKqpSinkPrecomputeTransformer(kqpCtx);
}

} // namespace NKikimr::NKqp::NOpt
