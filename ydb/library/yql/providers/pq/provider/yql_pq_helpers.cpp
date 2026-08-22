#include "yql_pq_helpers.h"
#include "yql_pq_settings.h"

#include <ydb/library/yql/providers/pq/common/yql_names.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

bool EnsureWatermarkExpr(const TPosition pos, TCoArgument arg, TExprBase body, TExprContext& ctx) {
    static constexpr std::string_view message = "Unrecognized watermark expression, please use WATERMARK = SystemMetadata('write_time') - Interval('PT5S')";

    const auto maybeMember = body.Maybe<TCoMember>();
    if (!maybeMember) {
        ctx.AddError(TIssue(pos, message));
        return false;
    }
    const auto member = maybeMember.Cast();

    const auto maybeArg = member.Struct().Maybe<TCoArgument>();
    if (!maybeArg) {
        ctx.AddError(TIssue(pos, message));
        return false;
    }

    if (maybeArg.Cast().Name() != arg.Name()) {
        ctx.AddError(TIssue(pos, message));
        return false;
    }
    if (!IsIn({"_yql_sys_tsp_write_time", "_yql_sys_write_time", "__ydb_write_time"}, member.Name())) {
        ctx.AddError(TIssue(pos, message));
        return false;
    }
    return true;
}

} // anonymous namespace

void Add(TVector<TCoNameValueTuple>& settings, TStringBuf name, TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(name)
        .Value<TCoAtom>().Build(value)
        .Done());
}

TCoNameValueTupleList BuildTopicPropsList(const TPqState::TTopicMeta& meta, TPositionHandle pos, TExprContext& ctx) {
    TVector<TCoNameValueTuple> props;

    ui32 maxPartitionsCount = 0;
    if (meta.FederatedTopic) {
        const auto& federatedTopics = *meta.FederatedTopic;
        if (federatedTopics.size() == 1 && federatedTopics[0].Info.Name.empty()) {
            // non-federated fallback, omit FederatedClusters
            maxPartitionsCount = federatedTopics[0].PartitionsCount;
        } else {
            TVector<TDqPqFederatedCluster> clusters(Reserve(federatedTopics.size()));
            for (const auto& topic: federatedTopics) {
                clusters.push_back(Build<TDqPqFederatedCluster>(ctx, pos)
                    .Name().Build(topic.Info.Name)
                    .Endpoint().Build(topic.Info.Endpoint)
                    .Database().Build(topic.Info.Path)
                    .PartitionsCount().Build(ToString(topic.PartitionsCount))
                    .Done());
                maxPartitionsCount = std::max(maxPartitionsCount, topic.PartitionsCount);
            }
            props.push_back(
                    Build<TCoNameValueTuple>(ctx, pos)
                    .Name().Build(FederatedClustersProp)
                    .Value<TDqPqFederatedClusterList>().Add(clusters).Build().Done());
        }
    }
    Add(props, PartitionsCountProp, ToString(maxPartitionsCount), pos, ctx);

    return Build<TCoNameValueTupleList>(ctx, pos)
        .Add(props)
        .Done();
}

void FindYdsDbIdsForResolving(
    const TPqState::TPtr& state,
    TExprNode::TPtr input,
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& ids)
{
    if (auto pqNodes = FindNodes(input, [&](const TExprNode::TPtr& node) {
        if (auto maybePqRead = TMaybeNode<TPqRead>(node)) {
            TPqRead read = maybePqRead.Cast();
            if (read.DataSource().Category().Value() == PqProviderName) {
                return true;
            }
        } else if (auto maybePqWrite = TMaybeNode<TPqWrite>(node)) {
            TPqWrite write = maybePqWrite.Cast();
            if (write.DataSink().Category().Value() == PqProviderName) {
                return true;
            }
        }
        return false;
    }); !pqNodes.empty()) {
        TString cluster;
        for (auto& node : pqNodes) {
            if (auto maybePqRead = TMaybeNode<TPqRead>(node)) {
                TPqRead read = maybePqRead.Cast();
                cluster = read.DataSource().Cluster().StringValue();
            } else if (auto maybePqWrite = TMaybeNode<TPqWrite>(node)) {
                TPqWrite write = maybePqWrite.Cast();
                cluster = write.DataSink().Cluster().StringValue();
            } else {
                Y_ABORT("Unrecognized pq node");
            }
            YQL_CLOG(INFO, ProviderPq) << "Found cluster: " << cluster;
            const auto& clusterCfgSettings = state->Configuration->ClustersConfigurationSettings;
            const auto foundSetting = clusterCfgSettings.find(cluster);
            if (foundSetting == clusterCfgSettings.end()
                || foundSetting->second.ClusterType != NYql::TPqClusterConfig::CT_DATA_STREAMS
                || foundSetting->second.Endpoint)
                continue;
            YQL_CLOG(INFO, ProviderPq) << "Found dbId: " << foundSetting->second.DatabaseId;
            if (!foundSetting->second.DatabaseId)
                continue;
            YQL_CLOG(INFO, ProviderPq) << "Resolve YDS id: " << foundSetting->second.DatabaseId;
            const auto idKey = std::make_pair(foundSetting->second.DatabaseId, NYql::EDatabaseType::DataStreams);
            const auto foundDbId = state->DatabaseIds.find(idKey);
            if (foundDbId != state->DatabaseIds.end()) {
                ids[idKey] = foundDbId->second;
            }
        }
    }
    YQL_CLOG(INFO, ProviderPq) << "Ids to resolve: " << ids.size();
}

void FillSettingsWithResolvedYdsIds(
    const TPqState::TPtr& state,
    const TDatabaseResolverResponse::TDatabaseDescriptionMap& fullResolvedIds)
{
    YQL_CLOG(INFO, ProviderPq) << "FullResolvedIds size: " << fullResolvedIds.size();
    auto& clusters = state->Configuration->ClustersConfigurationSettings;
    const auto& id2Clusters = state->Configuration->DbId2Clusters;
    for (const auto& [dbIdWithType, info] : fullResolvedIds) {
        const auto& dbId = dbIdWithType.first;
        YQL_CLOG(INFO, ProviderPq) << "DbId = " << dbId;
        const auto iter = id2Clusters.find(dbId);
        if (iter == id2Clusters.end()) {
            continue;
        }
        for (const auto& clusterName : iter->second) {
            auto& setting = clusters[clusterName];
            setting.Endpoint = info.Endpoint;
            setting.Database = info.Database;
            setting.UseSsl = info.Secure;
            state->Gateway->UpdateClusterConfigs(clusterName, info.Endpoint, info.Database, info.Secure);
        }
    }
}

TMaybeNode<TExprBase> FindSetting(TExprNode::TPtr settings, TStringBuf name) {
    const auto maybeSettingsList = TMaybeNode<TCoNameValueTupleList>(settings);
    if (!maybeSettingsList) {
        return nullptr;
    }
    const auto settingsList = maybeSettingsList.Cast();

    for (size_t i = 0; i < settingsList.Size(); ++i) {
        TCoNameValueTuple setting = settingsList.Item(i);
        if (setting.Name().Value() == name) {
            return setting.Value();
        }
    }
    return nullptr;
}

TMaybe<std::pair<TCoLambda, ui64>> SplitWatermarkExpr(
    const NNodes::TCoLambda& watermark,
    const TPqState& state,
    TExprContext& ctx
) {
    const auto pos = ctx.GetPosition(watermark.Pos());

    if (!state.EnableWatermarks && !state.EnableWatermarksAdvanced) {
        ctx.AddError(TIssue(pos, "Watermarks are disabled"));
        return Nothing();
    }

    static constexpr std::string_view message = "Incorrect watermark expression";
    const auto args = watermark.Args();
    if (args.Size() != 1) {
        ctx.AddError(TIssue(pos, message));
        return Nothing();
    }
    const auto arg = args.Arg(0);

    const auto body = watermark.Body();
    const auto maybeSub = body.Maybe<TCoSub>();
    if (!maybeSub) {
        ctx.AddError(TIssue(pos, message));
        return Nothing();
    }
    const auto sub = maybeSub.Cast();

    if (state.EnableWatermarks && !state.EnableWatermarksAdvanced && !EnsureWatermarkExpr(pos, arg, sub.Left(), ctx)) {
        return Nothing();
    }

    auto maybeInterval = sub.Right().Maybe<TCoInterval>();
    if (!maybeInterval) {
        ctx.AddError(TIssue(pos, message));
        return Nothing();
    }
    auto interval = maybeInterval.Cast();

    auto delay = TryFromString<ui64>(interval.Literal().Value());
    if (!delay) {
        ctx.AddError(TIssue(pos, message));
        return Nothing();
    }

    const auto newArg = Build<TCoArgument>(ctx, arg.Pos())
        .Name(arg.Name())
        .Done();
    return std::pair{
        Build<TCoLambda>(ctx, watermark.Pos())
            .Args({newArg})
            .Body(ctx.ReplaceNode(sub.Left().Ptr(), arg.Ref(), newArg.Ptr()))
            .Done(),
        *delay
    };
}

} // namespace NYql
