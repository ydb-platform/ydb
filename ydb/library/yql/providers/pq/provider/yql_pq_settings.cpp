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
        AddCluster(cluster, databaseIds, typeCtx->Credentials, dbResolver, {});
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

void TPqConfiguration::AddCluster(
    const NYql::TPqClusterConfig& cluster,
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& databaseIds,
    const TCredentials::TPtr& credentials,
    const std::shared_ptr<NYql::IDatabaseAsyncResolver>& dbResolver,
    const THashMap<TString, TString>& properties) {
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
        clusterSettings.SharedReading = cluster.GetSharedReading();
        clusterSettings.ReadGroup = cluster.GetReadGroup();

        const TString authToken = credentials->FindCredentialContent("cluster:default_" + clusterSettings.ClusterName, "default_pq", cluster.GetToken());
        clusterSettings.AuthToken = authToken;

        TString structuredTokenJson;
        auto authMethod = properties.Value("authMethod", "");
        if (authMethod == "TOKEN") {
            const TString& token = properties.Value("token", "");
            structuredTokenJson = ComposeStructuredTokenJsonForTokenAuthWithSecret(properties.Value("tokenReference", ""), token);
        } else if (authMethod == "BASIC") {
            const TString& login = properties.Value("login", "");
            const TString& password = properties.Value("password", "");
            const TString& passwordReference = properties.Value("passwordReference", "");
            structuredTokenJson = ComposeStructuredTokenJsonForBasicAuthWithSecret(login, passwordReference, password);
        } else {
            structuredTokenJson = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken);
        }
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

} // NYql
