#include "yql_pq_helpers.h"

#include "yql_pq_provider_impl.h"
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>

namespace NYql {

using namespace NNodes;

void Add(TVector<TCoNameValueTuple>& settings, TStringBuf name, TStringBuf value, TPositionHandle pos, TExprContext& ctx) {
    settings.push_back(Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(name)
        .Value<TCoAtom>().Build(value)
        .Done());
}

TCoNameValueTupleList BuildTopicPropsList(const TPqState::TTopicMeta& meta, TPositionHandle pos, TExprContext& ctx) {
    TVector<TCoNameValueTuple> props;

    Add(props, PartitionsCountProp, ToString(meta.Description->PartitionsCount), pos, ctx);

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


} // namespace NYql
