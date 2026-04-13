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

        if (!KqpCtx->Config->GetEnableOltpSink() || KqpCtx->Config->GetEnableIndexStreamWrite()) {
            return TStatus::Ok;
        }

        TNodeOnNodeOwnedMap marked;

        {
            const auto [precomputeStages, sinkStages] = GatherPrecomputeAndSinkStages(outputExpr, *KqpCtx);

            for (const auto& [_, exprNode] : FindStagesUsedForBothStagesSets(precomputeStages, sinkStages)) {
                AFL_ENSURE(exprNode);
                TExprBase node(exprNode);
                const auto stage = node.Cast<TDqStage>();
                if (HasNonDeterministicFunction(stage) || !IsKqpPureInputs(stage.Inputs())) {
                    marked.emplace(node.Raw(), node.Ptr());
                }
            }

            const auto resultStages = GatherResultStages(outputExpr, ctx);
            if (!resultStages) {
                return TStatus::Error;
            }

            for (const auto& [_, exprNode] : FindStagesUsedForBothStagesSets(*resultStages, sinkStages)) {
                AFL_ENSURE(exprNode);
                TExprBase node(exprNode);
                const auto stage = node.Cast<TDqStage>();
                if (!IsKqpPureInputs(stage.Inputs())) {
                    marked.emplace(node.Raw(), node.Ptr());
                }
            }
        }

        if (marked.empty()) {
            return TStatus::Ok;
        }

        // Find all stages that depend on marked stages
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
                                AFL_ENSURE(stage.Inputs().Item(0).Maybe<TDqCnUnionAll>());
                                AFL_ENSURE(stage.Settings().Empty());

                                AFL_ENSURE(stage.Program().Args().Size() == 1);
                                AFL_ENSURE(stage.Program().Body().Maybe<TCoArgument>());
                                AFL_ENSURE(stage.Program().Body().Cast<TCoArgument>().Name() ==
                                    stage.Program().Args().Arg(0).Cast<TCoArgument>().Name());

                                auto inputRows = Build<TDqPhyPrecompute>(ctx, node.Pos())
                                    .Connection(stage.Inputs().Item(0).Ptr())
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

        return TStatus(TStatus::Repeat, true);
    }

    void Rewind() final {
    }

private:
    TNodeOnNodeOwnedMap FindStagesUsedForBothStagesSets(
            const TNodeOnNodeOwnedMap& leftStages,
            const TNodeOnNodeOwnedMap& rightStages) {
        TNodeSet stagesUsedForLeft;
        for (const auto& leftStage : leftStages) {
            auto visit = [&stagesUsedForLeft](const TDqStage& stage) mutable -> bool {
                return stagesUsedForLeft.emplace(stage.Raw()).second;
            };
            auto postVisit = [](const TDqStage&) {};
            VisitStagesBackwards(leftStage.second, visit, postVisit);
        }

        TNodeOnNodeOwnedMap stagesUsedForLeftAndRight;
        TNodeSet stagesUsedForRight;
        for (const auto& rightStage : rightStages) {
            auto visit = [&stagesUsedForRight, &stagesUsedForLeft, &stagesUsedForLeftAndRight](
                            const TDqStage& stage) mutable -> bool {
                if (stagesUsedForLeft.contains(stage.Raw())) {
                    stagesUsedForLeftAndRight.emplace(stage.Raw(), stage.Ptr());
                }
                return stagesUsedForRight.emplace(stage.Raw()).second;
            };
            auto postVisit = [](const TDqStage&) {};
            VisitStagesBackwards(rightStage.second, visit, postVisit);
        }

        return stagesUsedForLeftAndRight;
    }

    std::pair<TNodeOnNodeOwnedMap, TNodeOnNodeOwnedMap> GatherPrecomputeAndSinkStages(const TExprNode::TPtr& query, const TKqpOptimizeContext& kqpCtx) {
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

            if (const auto outputs = stage.Outputs()) {
                ui64 sinkOutputsCount = 0;
                for (const auto& output : outputs.Cast()) {
                    if (auto maybeSink = output.Maybe<TDqSink>()) {
                        const auto sink = maybeSink.Cast();
                        if (const auto sinkSettings = sink.Settings().Maybe<TKqpTableSinkSettings>()) {
                            sinkOutputsCount++;
                            const auto executedAsSingleEffect = sinkSettings.Cast().Mode() == "fill_table"
                                || (kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, sinkSettings.Cast().Table().Path()).Metadata->Kind == EKikimrTableKind::Olap);
                            if (!executedAsSingleEffect) {
                                sinkStages.emplace(stage.Raw(), stage.Ptr());
                            }
                        }
                    }
                }

                AFL_ENSURE(sinkOutputsCount <= 1)("SinkOutputsCount", sinkOutputsCount);
            }

            return true;
        };

        VisitExpr(query, filter, gather);

        return {std::move(precomputeStages), std::move(sinkStages)};
    }

    std::optional<TNodeOnNodeOwnedMap> GatherResultStages(
            const TExprNode::TPtr& inputExpr,
            TExprContext& ctx) {
        TNodeOnNodeOwnedMap resultStages;
        TKqlQuery query(inputExpr);
        for (const auto& result : query.Results()) {
            if (auto maybeUnionAll = result.Value().Maybe<TDqCnUnionAll>()) {
                auto resultConnection = maybeUnionAll.Cast();
                auto resultStage = resultConnection.Output().Stage().Cast<TDqStage>();
                resultStages.emplace(resultStage.Raw(), resultStage.Ptr());
            } else if (!result.Value().Maybe<TDqCnValue>()) {
                ctx.AddError(TIssue(ctx.GetPosition(result.Pos()), TStringBuilder()
                    << "Unexpected node in results: " << KqpExprToPrettyString(result.Value(), ctx)));
                return std::nullopt;
            }
        }
        return resultStages;
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
