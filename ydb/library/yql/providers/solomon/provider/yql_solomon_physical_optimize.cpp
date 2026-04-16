#include "yql_solomon_provider_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <util/string/split.h>

namespace NYql {

namespace {

using namespace NNodes;
using namespace NDq;

TString FormatShardPathInvalid(const TSolomonClusterConfig& clusterConfig, const TString& path) {
    TStringBuilder err;
    err << "Invalid shard path " << path << " It should be ";
    switch (clusterConfig.GetClusterType()) {
        case TSolomonClusterConfig::SCT_SOLOMON:
            err << "{project}/{cluster}/{service}";
            break;
        case TSolomonClusterConfig::SCT_MONITORING:
            err << "{cloudId}/{folderId}/{service}";
            break;
        default:
            YQL_ENSURE(false, "Invalid cluster type " << ToString<ui32>(clusterConfig.GetClusterType()));
    }
    err << " or just {service} when using connection.";
    return err;
}

void ParseShardPath(
    const TSolomonClusterConfig& clusterConfig,
    const TString& path,
    TString& project,
    TString& cluster,
    TString& service)
{
    std::vector<TString> shardData;
    shardData.reserve(3);
    for (const auto& it : StringSplitter(path).Split('/')) {
        shardData.emplace_back(it.Token());
    }
    YQL_ENSURE(shardData.size() == 1 || shardData.size() == 3, "" << FormatShardPathInvalid(clusterConfig, path));

    project = shardData.size() == 3 ? shardData.at(0) : TString();
    cluster = shardData.size() == 3 ? shardData.at(1) : TString();
    service = shardData.back();
}

class TSoPhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    explicit TSoPhysicalOptProposalTransformer(TSolomonState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderSolomon, {})
        , State_(std::move(state))
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TSoPhysicalOptProposalTransformer::name)
        if (!State_->WriteThroughDqIntegration) {
            AddHandler(0, &TSoWriteToShard::Match, HNDL(SoWriteToShard));
        }
        AddHandler(0, &TSoInsert::Match, HNDL(SoInsert));
        AddHandler(0, &TCoLeft::Match, HNDL(TrimReadWorld));
#undef HNDL

        SetGlobal(0); // Stage 0 of this optimizer is global => we can remap nodes.
    }

    TMaybeNode<TExprBase> TrimReadWorld(TExprBase node, TExprContext& ctx) const {
        Y_UNUSED(ctx);

        const auto& maybeRead = node.Cast<TCoLeft>().Input().Maybe<TSoReadObject>();
        if (!maybeRead) {
            return node;
        }

        return TExprBase(maybeRead.Cast().World().Ptr());
    }

    TMaybe<TDqStage> BuildSinkStage(TPositionHandle writePos, TSoDataSink dataSink, TCoAtom writeShard, TExprBase input, TExprContext& ctx, const TGetParents& getParents, bool allowPureStage) const {
        const auto typeAnn = input.Ref().GetTypeAnn();
        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<false, true, false>(input.Pos(), *typeAnn, ctx, &inputItemType)) {
            return {};
        }

        const TExprBase rowTypeNode(ExpandType(writePos, *inputItemType, ctx));
        const TString solomonCluster(dataSink.Cluster());
        auto dqSinkBuilder = Build<TDqSink>(ctx, writePos)
            .DataSink(dataSink)
            .Settings(BuildSolomonShard(writeShard, rowTypeNode, ctx, solomonCluster));

        if (allowPureStage && IsDqPureExpr(input)) {
            YQL_CLOG(INFO, ProviderSolomon) << "Optimize insert into solomon (SoWriteToShard / SoInsert), build pure stage with sink";

            const auto dqSink = dqSinkBuilder
                .Index().Build(0)
                .Done();

            return Build<TDqStage>(ctx, writePos)
                .Inputs().Build()
                .Program<TCoLambda>()
                    .Args({})
                    .Body<TCoToFlow>()
                        .Input(input)
                        .Build()
                    .Build()
                .Outputs()
                    .Add(dqSink)
                    .Build()
                .Settings().Build()
                .Done();
        }
        
        const auto maybeDqUnion = input.Maybe<TDqCnUnionAll>();
        if (!maybeDqUnion) {
            // If input is not DqCnUnionAll, it means not all dq optimizations are done yet
            return {};
        }

        const auto dqUnion = maybeDqUnion.Cast();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *getParents())) {
            return {};
        }

        YQL_CLOG(INFO, ProviderSolomon) << "Optimize insert into solomon (SoWriteToShard / SoInsert), push into existing stage";

        const auto dqSink = dqSinkBuilder
            .Index(dqUnion.Output().Index())
            .Done();

        const auto inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        auto sinksBuilder = Build<TDqStageOutputsList>(ctx, inputStage.Pos());
        if (inputStage.Outputs()) {
            sinksBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        sinksBuilder.Add(dqSink);

        return Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Outputs(sinksBuilder.Done())
            .Done();
    }

    TMaybeNode<TExprBase> SoWriteToShard(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
        if (State_->IsRtmrMode()) {
            return node;
        }

        const auto write = node.Cast<TSoWriteToShard>();
        const auto stage = BuildSinkStage(write.Pos(), write.DataSink(), write.Shard(), write.Input(), ctx, getParents, /* allowPureStage */ false);
        if (!stage) {
            return node;
        }

        return Build<TDqQuery>(ctx, write.Pos())
            .World(write.World())
            .SinkStages()
                .Add(*stage)
                .Build()
            .Done();
    }

    TMaybeNode<TExprBase> SoInsert(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        const auto insert = node.Cast<TSoInsert>();
        const auto input = insert.Input();
        const auto maybeStage = BuildSinkStage(insert.Pos(), insert.DataSink(), insert.Shard(), input, ctx, getParents, /* allowPureStage */ true);
        if (!maybeStage) {
            return node;
        }

        const auto newStage = *maybeStage;
        YQL_ENSURE(newStage.Outputs());
        const auto outputsCount = newStage.Outputs().Cast().Size();
        if (outputsCount > 1) {
            YQL_ENSURE(newStage.Program().Body().Maybe<TDqReplicate>(), "Can not push multiple async outputs into stage without TDqReplicate");
        }

        const auto maybeDqUnion = input.Maybe<TDqCnUnionAll>();
        if (!maybeDqUnion) {
            YQL_ENSURE(outputsCount == 1, "Expected only one sink for pure stage");
            return newStage;
        }

        const auto dqUnionOutput = maybeDqUnion.Cast().Output();
        optCtx.RemapNode(dqUnionOutput.Stage().Ref(), newStage.Ptr());

        const auto dqResult = Build<TCoNth>(ctx, newStage.Pos())
            .Tuple(newStage)
            .Index(dqUnionOutput.Index())
            .Done();

        return ctx.NewList(dqResult.Pos(), {dqResult.Ptr()});
    }

private:
    TCallable BuildSolomonShard(TCoAtom shardNode, TExprBase rowType, TExprContext& ctx, TString solomonCluster) const {
        const auto* clusterDesc = State_->Configuration->ClusterConfigs.FindPtr(solomonCluster);
        YQL_ENSURE(clusterDesc, "Unknown cluster " << solomonCluster);

        TString project, cluster, service;
        ParseShardPath(*clusterDesc, shardNode.StringValue(), project, cluster, service);

        if (project.empty() && clusterDesc->HasPath()) {
            project = clusterDesc->GetPath().GetProject();
        }
        YQL_ENSURE(!project.empty(), "Project is not defined. You can define it inside connection, or inside query.");

        if (cluster.empty() && clusterDesc->HasPath()) {
            cluster = clusterDesc->GetPath().GetCluster();
        }
        YQL_ENSURE(!cluster.empty(), "Cluster is not defined. You can define it inside connection, or inside query.");

        return Build<TSoShard>(ctx, shardNode.Pos())
            .SolomonCluster<TCoAtom>().Value(solomonCluster).Build()
            .Project<TCoAtom>().Value(project).Build()
            .Cluster<TCoAtom>().Value(cluster).Build()
            .Service<TCoAtom>().Value(service).Build()
            .RowType(rowType)
            .Token<TCoSecureParam>().Name().Build("cluster:default_" + solomonCluster).Build()
            .Done();
    }

private:
    TSolomonState::TPtr State_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreateSoPhysicalOptProposalTransformer(TSolomonState::TPtr state) {
    return MakeHolder<TSoPhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
