#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    struct TGenericSettings {
        using TConstPtr = std::shared_ptr<const TGenericSettings>;
    };

    struct TGenericURL {
        TString Host;
        ui16 Port;
        EHostScheme Scheme;

        TString Endpoint() const {
            return Host + ':' + ToString(Port);
        };
    };

    struct TGenericConfiguration: public TGenericSettings, public NCommon::TSettingDispatcher {
        using TPtr = TIntrusivePtr<TGenericConfiguration>;

        TGenericConfiguration();
        TGenericConfiguration(const TGenericConfiguration&) = delete;

        template <typename TProtoConfig>
        void Init(const TProtoConfig& config, const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
                  THashMap<std::pair<TString, NYql::DatabaseType>, NYql::TDatabaseAuth>& databaseIds)
        {
            // TODO: support data sources other than ClickHouse here
            TVector<TString> clusters(Reserve(config.GetClickHouse().ClusterMappingSize()));
            for (auto& cluster : config.GetClickHouse().GetClusterMapping()) {
                clusters.push_back(cluster.GetName());
            }

            this->SetValidClusters(clusters);

            // TODO: support data sources other than ClickHouse here
            this->Dispatch(config.GetClickHouse().GetDefaultSettings());
            for (auto& cluster : config.GetClickHouse().GetClusterMapping()) {
                this->Dispatch(cluster.GetName(), cluster.GetSettings());

                if (dbResolver) {
                    YQL_CLOG(DEBUG, ProviderGeneric)
                        << "Settings: clusterName = " << cluster.GetName() << ", clusterDbId = " << cluster.GetId()
                        << ", cluster.GetCluster(): " << cluster.GetCluster()
                        << ", HasCluster: " << (cluster.HasCluster() ? "TRUE" : "FALSE");
                    databaseIds[std::make_pair(cluster.GetId(), NYql::DatabaseType::Generic)] =
                        NYql::TDatabaseAuth{cluster.GetCHToken(), /*AddBearer=*/false};
                    if (cluster.GetId()) {
                        DbId2Clusters[cluster.GetId()].emplace_back(cluster.GetName());
                        YQL_CLOG(DEBUG, ProviderGeneric) << "Add dbId: " << cluster.GetId() << " to DbId2Clusters";
                    }
                }

                Tokens[cluster.GetName()] = cluster.GetCHToken();
                // TODO: Drop later
                TString endpoint;
                if (cluster.HasCluster()) {
                    endpoint = cluster.GetCluster();
                    if (endpoint.StartsWith("https://")) {
                        endpoint = endpoint.substr(8);
                    }
                    endpoint = endpoint.substr(0, endpoint.find(':'));
                } else {
                    endpoint = cluster.GetId();
                }
                Endpoints[cluster.GetName()] =
                    std::make_pair(endpoint + ":" + ToString(cluster.GetNativeHostPort()), cluster.GetNativeSecure());

                auto& url = Urls[cluster.GetName()];
                auto& host = url.Host;
                auto& scheme = url.Scheme;
                auto& port = url.Port;
                host = cluster.GetCluster();
                while (host.EndsWith("/"))
                    host = host.substr(0u, host.length() - 1u);
                if (host.StartsWith("http://")) {
                    scheme = HS_HTTP;
                    host = host.substr(7u);
                    port = 80;
                } else {
                    scheme = HS_HTTPS;
                    port = 443;
                    if (host.StartsWith("https://")) {
                        host = host.substr(8u);
                    }
                }

                if (const auto p = host.rfind(':'); TString::npos != p) {
                    port = ::FromString<ui16>(host.substr(p + 1u));
                    host = host.substr(0u, p);
                }

                if (cluster.HasHostScheme())
                    scheme = cluster.GetHostScheme();
                if (cluster.HasHostPort())
                    port = cluster.GetHostPort();
            }
            this->FreezeDefaults();
        }

        bool HasCluster(TStringBuf cluster) const;

        TGenericSettings::TConstPtr Snapshot() const;
        THashMap<TString, TString> Tokens;
        THashMap<TString, TGenericURL> Urls;
        THashMap<TString, std::pair<TString, bool>> Endpoints;
        THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
    };

}
