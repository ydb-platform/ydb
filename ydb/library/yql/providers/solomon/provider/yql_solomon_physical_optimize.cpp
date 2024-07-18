#include "yql_solomon_provider_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/solomon/expr_nodes/yql_solomon_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

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
        AddHandler(0, &TSoWriteToShard::Match, HNDL(SoWriteToShard));
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

    TMaybeNode<TExprBase> SoWriteToShard(TExprBase node, TExprContext& ctx, IOptimizationContext& optCtx, const TGetParents& getParents) const {
        if (State_->IsRtmrMode()) {
            return node;
        }

        auto write = node.Cast<TSoWriteToShard>();
        if (!TDqCnUnionAll::Match(write.Input().Raw())) {
            // If input is not DqCnUnionAll, it means not all dq optimizations are done yet
            return node;
        }

        const TParentsMap* parentsMap = getParents();
        auto dqUnion = write.Input().Cast<TDqCnUnionAll>();
        if (!NDq::IsSingleConsumerConnection(dqUnion, *parentsMap)) {
            return node;
        }

        YQL_CLOG(INFO, ProviderSolomon) << "Optimize SoWriteToShard";

        const auto solomonCluster = TString(write.DataSink().Cluster().Value());
        auto* typeAnn = write.Input().Ref().GetTypeAnn();
        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<false, true, false>(write.Input().Pos(), *typeAnn, ctx, &inputItemType)) {
            return {};
        }

        auto rowTypeNode = ExpandType(write.Pos(), *inputItemType, ctx);
        auto shard = BuildSolomonShard(write.Shard().Cast<TCoAtom>(), TExprBase(rowTypeNode), ctx, solomonCluster);

        auto dqSink = Build<TDqSink>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Settings(shard)
            .Index(dqUnion.Output().Index())
            .Done();

        TDqStage inputStage = dqUnion.Output().Stage().Cast<TDqStage>();

        auto sinksBuilder = Build<TDqStageOutputsList>(ctx, inputStage.Pos());
        if (inputStage.Outputs()) {
            sinksBuilder.InitFrom(inputStage.Outputs().Cast());
        }
        sinksBuilder.Add(dqSink);

        auto dqStageWithSink = Build<TDqStage>(ctx, inputStage.Pos())
            .InitFrom(inputStage)
            .Outputs(sinksBuilder.Done())
            .Done();

        auto dqQueryBuilder = Build<TDqQuery>(ctx, write.Pos());
        dqQueryBuilder.World(write.World());
        dqQueryBuilder.SinkStages().Add(dqStageWithSink).Build();

        optCtx.RemapNode(inputStage.Ref(), dqStageWithSink.Ptr());

        return dqQueryBuilder.Done();
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

} // namespace

THolder<IGraphTransformer> CreateSoPhysicalOptProposalTransformer(TSolomonState::TPtr state) {
    return MakeHolder<TSoPhysicalOptProposalTransformer>(std::move(state));
}

} // namespace NYql
