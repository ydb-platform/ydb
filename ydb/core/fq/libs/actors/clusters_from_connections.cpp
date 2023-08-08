#include "clusters_from_connections.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/system/env.h>

#include <library/cpp/string_utils/quote/quote.h>

namespace NFq {

using namespace NYql;

namespace {

template <typename TClusterConfig>
void FillClusterAuth(TClusterConfig& clusterCfg,
        const FederatedQuery::IamAuth& auth, const TString& authToken,
        const THashMap<TString, TString>& accountIdSignatures) {
    switch (auth.identity_case()) {
    case FederatedQuery::IamAuth::kNone:
        break;
    case FederatedQuery::IamAuth::kCurrentIam:
        clusterCfg.SetToken(authToken);
        break;
    case FederatedQuery::IamAuth::kServiceAccount:
        clusterCfg.SetServiceAccountId(auth.service_account().id());
        clusterCfg.SetServiceAccountIdSignature(accountIdSignatures.at(auth.service_account().id()));
        break;
    // Do not replace with default. Adding a new auth item should cause a compilation error
    case FederatedQuery::IamAuth::IDENTITY_NOT_SET:
        break;
    }
}

void FillPqClusterConfig(NYql::TPqClusterConfig& clusterConfig,
        const TString& name, bool useBearerForYdb,
        const TString& authToken, const THashMap<TString, TString>& accountIdSignatures,
        const FederatedQuery::DataStreams& ds) {
    clusterConfig.SetName(name);
    if (ds.endpoint()) {
        clusterConfig.SetEndpoint(ds.endpoint());
    }
    clusterConfig.SetDatabase(ds.database());
    clusterConfig.SetDatabaseId(ds.database_id());
    clusterConfig.SetUseSsl(ds.secure());
    clusterConfig.SetAddBearerToToken(useBearerForYdb);
    clusterConfig.SetClusterType(TPqClusterConfig::CT_DATA_STREAMS);
    FillClusterAuth(clusterConfig, ds.auth(), authToken, accountIdSignatures);
}

void FillS3ClusterConfig(NYql::TS3ClusterConfig& clusterConfig,
        const TString& name, const TString& authToken,
        const TString& objectStorageEndpoint,
        const THashMap<TString, TString>& accountIdSignatures,
        const FederatedQuery::ObjectStorageConnection& s3) {
    clusterConfig.SetName(name);
    TString objectStorageUrl;


    if (objectStorageEndpoint == "https://s3.mds.yandex.net") {
        TUrlBuilder builder{"https://"};
        objectStorageUrl = builder.AddPathComponent(s3.bucket() + ".s3.mds.yandex.net/").Build();
    } else {
        TUrlBuilder builder{UrlEscapeRet(objectStorageEndpoint, true)};
        objectStorageUrl = builder.AddPathComponent(s3.bucket() + "/").Build();
    }
    clusterConfig.SetUrl(objectStorageUrl);
    FillClusterAuth(clusterConfig, s3.auth(), authToken, accountIdSignatures);
}

void FillSolomonClusterConfig(NYql::TSolomonClusterConfig& clusterConfig,
    const TString& name,
    const TString& authToken,
    const TString& endpoint,
    const THashMap<TString, TString>& accountIdSignatures,
    const FederatedQuery::Monitoring& monitoring) {
    clusterConfig.SetName(name);

    clusterConfig.SetCluster(endpoint);
    clusterConfig.SetClusterType(TSolomonClusterConfig::SCT_MONITORING);
    clusterConfig.MutablePath()->SetProject(monitoring.project());
    clusterConfig.MutablePath()->SetCluster(monitoring.cluster());
    FillClusterAuth(clusterConfig, monitoring.auth(), authToken, accountIdSignatures);
}

template <typename TConnection>
void FillGenericClusterConfig(
    NYql::TGenericClusterConfig& clusterCfg,
    const TConnection& connection,
    const TString& connectionName,
    NConnector::NApi::EDataSourceKind dataSourceKind,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures
){
        clusterCfg.SetKind(dataSourceKind);
        clusterCfg.SetName(connectionName);
        clusterCfg.SetDatabaseId(connection.database_id());
        clusterCfg.mutable_credentials()->mutable_basic()->set_username(connection.login());
        clusterCfg.mutable_credentials()->mutable_basic()->set_password(connection.password());
        FillClusterAuth(clusterCfg, connection.auth(), authToken, accountIdSignatures);
        // Since resolver always returns secure ports, we'll always ask for secure connections
        // between remote Connector and the data source:
        // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/db_id_async_resolver_impl/mdb_host_transformer.cpp#L24
        clusterCfg.SetUseSsl(true);
}

} //namespace

NYql::TPqClusterConfig CreatePqClusterConfig(const TString& name,
        bool useBearerForYdb, const TString& authToken,
        const TString& accountSignature, const FederatedQuery::DataStreams& ds) {
    NYql::TPqClusterConfig cluster;
    THashMap<TString, TString> accountIdSignatures;
    if (ds.auth().has_service_account()) {
        accountIdSignatures[ds.auth().service_account().id()] = accountSignature;
    }
    FillPqClusterConfig(cluster, name, useBearerForYdb, authToken, accountIdSignatures, ds);
    return cluster;
}

NYql::TS3ClusterConfig CreateS3ClusterConfig(const TString& name,
        const TString& authToken, const TString& objectStorageEndpoint,
        const TString& accountSignature, const FederatedQuery::ObjectStorageConnection& s3) {
    NYql::TS3ClusterConfig cluster;
    THashMap<TString, TString> accountIdSignatures;
    accountIdSignatures[s3.auth().service_account().id()] = accountSignature;
    FillS3ClusterConfig(cluster, name, authToken, objectStorageEndpoint, accountIdSignatures, s3);
    return cluster;
}

NYql::TSolomonClusterConfig CreateSolomonClusterConfig(const TString& name,
        const TString& authToken,
        const TString& endpoint,
        const TString& accountSignature,
        const FederatedQuery::Monitoring& monitoring) {
    NYql::TSolomonClusterConfig cluster;
    THashMap<TString, TString> accountIdSignatures;
    accountIdSignatures[monitoring.auth().service_account().id()] = accountSignature;
    FillSolomonClusterConfig(cluster, name, authToken, endpoint, accountIdSignatures, monitoring);
    return cluster;
}

void AddClustersFromConnections(
    const THashMap<TString, FederatedQuery::Connection>& connections,
    bool useBearerForYdb,
    const TString& objectStorageEndpoint,
    const TString& monitoringEndpoint,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures,
    TGatewaysConfig& gatewaysConfig,
    THashMap<TString, TString>& clusters)
{
    for (const auto&[_, conn] : connections) {
        auto connectionName = conn.content().name();
        switch (conn.content().setting().connection_case()) {
        case FederatedQuery::ConnectionSetting::kYdbDatabase: {
            const auto& db = conn.content().setting().ydb_database();
            auto* clusterCfg = gatewaysConfig.MutableYdb()->AddClusterMapping();
            clusterCfg->SetName(connectionName);
            clusterCfg->SetId(db.database_id());
            if (db.database())
                clusterCfg->SetDatabase(db.database());
            if (db.endpoint())
                clusterCfg->SetEndpoint(db.endpoint());
            clusterCfg->SetSecure(db.secure());
            clusterCfg->SetAddBearerToToken(useBearerForYdb);
            FillClusterAuth(*clusterCfg, db.auth(), authToken, accountIdSignatures);
            clusters.emplace(connectionName, YdbProviderName);
            break;
        }
        case FederatedQuery::ConnectionSetting::kClickhouseCluster: {
            FillGenericClusterConfig(
                *gatewaysConfig.MutableGeneric()->AddClusterMapping(),
                conn.content().setting().clickhouse_cluster(),
                connectionName,
                NYql::NConnector::NApi::EDataSourceKind::CLICKHOUSE,
                authToken,
                accountIdSignatures);
            clusters.emplace(connectionName, GenericProviderName);
            break;
        }
        case FederatedQuery::ConnectionSetting::kObjectStorage: {
            const auto& s3 = conn.content().setting().object_storage();
            auto* clusterCfg = gatewaysConfig.MutableS3()->AddClusterMapping();
            FillS3ClusterConfig(*clusterCfg, connectionName, authToken, objectStorageEndpoint, accountIdSignatures, s3);
            clusters.emplace(connectionName, S3ProviderName);
            break;
        }
        case FederatedQuery::ConnectionSetting::kDataStreams: {
            const auto& ds = conn.content().setting().data_streams();
            auto* clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
            FillPqClusterConfig(*clusterCfg, connectionName, useBearerForYdb, authToken, accountIdSignatures, ds);
            clusters.emplace(connectionName, PqProviderName);
            break;
        }
        case FederatedQuery::ConnectionSetting::kMonitoring: {
            const auto& monitoring = conn.content().setting().monitoring();
            auto* clusterCfg = gatewaysConfig.MutableSolomon()->AddClusterMapping();
            FillSolomonClusterConfig(*clusterCfg, connectionName, authToken, monitoringEndpoint, accountIdSignatures, monitoring);
            clusters.emplace(connectionName, SolomonProviderName);
            break;
        }
        case FederatedQuery::ConnectionSetting::kPostgresqlCluster: {
            FillGenericClusterConfig(
                *gatewaysConfig.MutableGeneric()->AddClusterMapping(),
                conn.content().setting().postgresql_cluster(),
                connectionName,
                NYql::NConnector::NApi::EDataSourceKind::POSTGRESQL,
                authToken,
                accountIdSignatures);
            clusters.emplace(connectionName, GenericProviderName);
            break;
        }

        // Do not replace with default. Adding a new connection should cause a compilation error
        case FederatedQuery::ConnectionSetting::CONNECTION_NOT_SET:
            break;
        }
    }
}
} //NFq
