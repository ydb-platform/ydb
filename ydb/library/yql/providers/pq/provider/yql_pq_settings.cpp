#include "yql_pq_settings.h"

namespace NYql {

using namespace NCommon;

TPqConfiguration::TPqConfiguration() {
    REGISTER_SETTING(*this, Consumer);
    REGISTER_SETTING(*this, Database);
    REGISTER_SETTING(*this, PqReadByRtmrCluster_);
}

TPqSettings::TConstPtr TPqConfiguration::Snapshot() const {
    return std::make_shared<const TPqSettings>(*this);
}

void TPqConfiguration::Init(
    const TPqGatewayConfig& config,
    TIntrusivePtr<TTypeAnnotationContext> typeCtx,
    const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& databaseIds)
{
    TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
    for (auto& cluster: config.GetClusterMapping()) {
        clusters.push_back(cluster.GetName());
    }

    SetValidClusters(clusters);

    Dispatch(config.GetDefaultSettings());

    for (auto& cluster: config.GetClusterMapping()) {
        Dispatch(cluster.GetName(), cluster.GetSettings());
        TPqClusterConfigurationSettings& clusterSettings = ClustersConfigurationSettings[cluster.GetName()];

        clusterSettings.ClusterName = cluster.GetName();
        clusterSettings.ClusterType = cluster.GetClusterType();
        clusterSettings.Endpoint = cluster.GetEndpoint();
        clusterSettings.ConfigManagerEndpoint = cluster.GetConfigManagerEndpoint();
        clusterSettings.Database = cluster.GetDatabase();
        clusterSettings.DatabaseId = cluster.GetDatabaseId();
        clusterSettings.TvmId = cluster.GetTvmId();
        clusterSettings.UseSsl = cluster.GetUseSsl();
        clusterSettings.AddBearerToToken = cluster.GetAddBearerToToken();

        const TString authToken = typeCtx->Credentials->FindCredentialContent("cluster:default_" + clusterSettings.ClusterName, "default_pq", cluster.GetToken());
        clusterSettings.AuthToken = authToken;
        const auto structuredTokenJson = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken);
        Tokens[clusterSettings.ClusterName] = structuredTokenJson;

        if (dbResolver) {
            YQL_CLOG(DEBUG, ProviderPq) << "Settings: clusterName = " << cluster.GetName()
                << ", clusterDbId = "  << cluster.GetDatabaseId() << ", cluster.GetEndpoint(): " << cluster.GetEndpoint() << ", HasEndpoint = " << (cluster.HasEndpoint() ? "TRUE" : "FALSE") ;
            if (cluster.GetDatabaseId()) {
                databaseIds[std::make_pair(cluster.GetDatabaseId(), NYql::EDatabaseType::DataStreams)] =
                    NYql::TDatabaseAuth{structuredTokenJson, cluster.GetAddBearerToToken()};
                DbId2Clusters[cluster.GetDatabaseId()].emplace_back(cluster.GetName());
                YQL_CLOG(DEBUG, ProviderPq) << "Add dbId: " << cluster.GetDatabaseId() << " to DbId2Clusters";
            }
        }
    }
    FreezeDefaults();
}

TString TPqConfiguration::GetDatabaseForTopic(const TString& cluster) const {
    if (TMaybe<TString> explicitDb = Database.Get()) {
        return *explicitDb;
    }
    const auto clusterSetting = ClustersConfigurationSettings.FindPtr(cluster);
    YQL_ENSURE(clusterSetting, "Unknown cluster " << cluster);
    return clusterSetting->Database;
}

} // NYql
