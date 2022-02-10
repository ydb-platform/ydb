#include "clusters_from_connections.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/system/env.h>

namespace NYq {

using namespace NYql;

namespace {

template <typename TClusterConfig>
void FillClusterAuth(TClusterConfig& clusterCfg, const YandexQuery::IamAuth& auth, const TString& authToken, const THashMap<TString, TString>& accountIdSignatures) {
    switch (auth.identity_case()) {
    case YandexQuery::IamAuth::kNone:
        break;
    case YandexQuery::IamAuth::kCurrentIam:
        clusterCfg.SetToken(authToken);
        break;
    case YandexQuery::IamAuth::kServiceAccount:
        clusterCfg.SetServiceAccountId(auth.service_account().id());
        clusterCfg.SetServiceAccountIdSignature(accountIdSignatures.at(auth.service_account().id()));
        break;
    // Do not replace with default. Adding a new auth item should cause a compilation error
    case YandexQuery::IamAuth::IDENTITY_NOT_SET:
        break;
    }
}

} //namespace

void AddClustersFromConnections(const THashMap<TString, YandexQuery::Connection>& connections,
    bool useBearerForYdb,
    const TString& objectStorageEndpoint,
    const TString& authToken,
    const THashMap<TString, TString>& accountIdSignatures,
    TGatewaysConfig& gatewaysConfig,
    THashMap<TString, TString>& clusters) {
    for (const auto&[_, conn] : connections) {
        auto connectionName = conn.content().name();
        switch (conn.content().setting().connection_case()) {
        case YandexQuery::ConnectionSetting::kYdbDatabase: {
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
        case YandexQuery::ConnectionSetting::kClickhouseCluster: {
            const auto& ch = conn.content().setting().clickhouse_cluster();
            auto* clusterCfg = gatewaysConfig.MutableClickHouse()->AddClusterMapping();
            clusterCfg->SetName(connectionName);
            clusterCfg->SetId(ch.database_id());
            if (ch.host())
                clusterCfg->SetCluster(ch.host());
            clusterCfg->SetNativeHostPort(9440);
            clusterCfg->SetNativeSecure(true);
            clusterCfg->SetCHToken(TStringBuilder() << "basic#" << ch.login() << "#" << ch.password());
            clusters.emplace(connectionName, ClickHouseProviderName);
            break;
        }
        case YandexQuery::ConnectionSetting::kObjectStorage: {
            const auto& s3 = conn.content().setting().object_storage();
            auto* clusterCfg = gatewaysConfig.MutableS3()->AddClusterMapping();
            clusterCfg->SetName(connectionName);
            TString objectStorageUrl;
            if (objectStorageEndpoint == "https://s3.mds.yandex.net") {
                objectStorageUrl = TStringBuilder() << "https://" << s3.bucket() << ".s3.mds.yandex.net/";
            } else {
                objectStorageUrl = TStringBuilder() << objectStorageEndpoint << '/' << s3.bucket() << '/';
            }
            clusterCfg->SetUrl(objectStorageUrl);
            FillClusterAuth(*clusterCfg, s3.auth(), authToken, accountIdSignatures);
            clusters.emplace(connectionName, S3ProviderName);
            break;
        }
        case YandexQuery::ConnectionSetting::kDataStreams: {
            const auto& ds = conn.content().setting().data_streams();
            auto* clusterCfg = gatewaysConfig.MutablePq()->AddClusterMapping();
            clusterCfg->SetName(connectionName);
            if (ds.endpoint())
                clusterCfg->SetEndpoint(ds.endpoint());
            clusterCfg->SetDatabase(ds.database());
            clusterCfg->SetDatabaseId(ds.database_id()); 
            clusterCfg->SetUseSsl(ds.secure());
            clusterCfg->SetAddBearerToToken(useBearerForYdb);
            clusterCfg->SetClusterType(TPqClusterConfig::CT_DATA_STREAMS);
            FillClusterAuth(*clusterCfg, ds.auth(), authToken, accountIdSignatures);
            clusters.emplace(connectionName, PqProviderName);
            break;
        }
        case YandexQuery::ConnectionSetting::kMonitoring: {
            const auto& monitoring = conn.content().setting().monitoring();
            auto* clusterCfg = gatewaysConfig.MutableSolomon()->AddClusterMapping();
            clusterCfg->SetName(connectionName);

            // TODO: move Endpoint to yq config
            auto solomonEndpointForTest = GetEnv("SOLOMON_ENDPOINT");
            auto solomonEndpoint = solomonEndpointForTest ? TString(solomonEndpointForTest) : TString();
            if (solomonEndpoint.empty()) {
                if (connectionName.StartsWith("pre")) {
                    solomonEndpoint = "monitoring.api.cloud-preprod.yandex.net";
                    clusterCfg->SetUseSsl(true);
                } else if (connectionName.StartsWith("so")) {
                    solomonEndpoint = "solomon.yandex.net";
                } else {
                    solomonEndpoint = "monitoring.api.cloud.yandex.net";
                    clusterCfg->SetUseSsl(true);
                }
            }

            clusterCfg->SetCluster(solomonEndpoint);
            clusterCfg->SetClusterType(TSolomonClusterConfig::SCT_MONITORING);
            clusterCfg->MutablePath()->SetProject(monitoring.project());
            clusterCfg->MutablePath()->SetCluster(monitoring.cluster());
            FillClusterAuth(*clusterCfg, monitoring.auth(), authToken, accountIdSignatures);
            clusters.emplace(connectionName, SolomonProviderName);
            break;
        }

        // Do not replace with default. Adding a new connection should cause a compilation error
        case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET:
            break;
        }
    }
}
} //NYq
