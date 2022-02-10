#include "yql_ydb_settings.h"
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

namespace NYql {

using namespace NCommon;

TYdbConfiguration::TYdbConfiguration()
{
}

TYdbSettings::TConstPtr TYdbConfiguration::Snapshot() const {
    return std::make_shared<const TYdbSettings>(*this);
}

bool TYdbConfiguration::HasCluster(TStringBuf cluster) const {
    return ValidClusters.contains(cluster);
}

void TYdbConfiguration::Init( 
    const TYdbGatewayConfig& config, 
    TIntrusivePtr<TTypeAnnotationContext> typeCtx, 
    const std::shared_ptr<NYq::TDatabaseAsyncResolverWithMeta> dbResolver, 
    THashMap<std::pair<TString, NYq::DatabaseType>, NYq::TEvents::TDatabaseAuth>& databaseIds) 
{
    TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
    for (auto& cluster: config.GetClusterMapping()) {
        clusters.push_back(cluster.GetName());
    }

    this->SetValidClusters(clusters);

    this->Dispatch(config.GetDefaultSettings());

    const auto& endpoint = config.GetDefaultEndpoint();

    for (const auto& cluster : config.GetClusterMapping()) {
        this->Dispatch(cluster.GetName(), cluster.GetSettings());
 
        if (dbResolver) { 
            dbResolver->TryAddDbIdToResolve(cluster.HasEndpoint(), cluster.GetName(), cluster.GetId(), NYq::DatabaseType::Ydb, databaseIds); 
            if (cluster.GetId()) { 
                DbId2Clusters[cluster.GetId()].emplace_back(cluster.GetName()); 
            } 
        } 
 
        auto& settings = Clusters[cluster.GetName()];
        settings.Endpoint = cluster.HasEndpoint() ? cluster.GetEndpoint() : endpoint;
        settings.Database = cluster.GetDatabase().empty() ? cluster.GetName() : cluster.GetDatabase();
        if (cluster.GetId()) 
            settings.DatabaseId = cluster.GetId(); 
        if (cluster.HasSecure())
            settings.Secure = cluster.GetSecure();
        if (cluster.HasAddBearerToToken())
            settings.AddBearerToToken = cluster.GetAddBearerToToken();

        const TString authToken = typeCtx->FindCredentialContent("cluster:default_" + cluster.GetName(), "default_ydb", cluster.GetToken());
        Tokens[cluster.GetName()] = ComposeStructuredTokenJsonForServiceAccount(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature(), authToken);
        settings.Raw = cluster;
 
    }
    this->FreezeDefaults();
}

} // NYql
