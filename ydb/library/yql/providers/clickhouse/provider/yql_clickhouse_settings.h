#pragma once

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>


namespace NYql {

struct TClickHouseSettings {
    using TConstPtr = std::shared_ptr<const TClickHouseSettings>;
};

struct TClickHouseConfiguration : public TClickHouseSettings, public NCommon::TSettingDispatcher {
    using TPtr = TIntrusivePtr<TClickHouseConfiguration>;

    TClickHouseConfiguration();
    TClickHouseConfiguration(const TClickHouseConfiguration&) = delete;

    template <typename TProtoConfig>
    void Init(
        const TProtoConfig& config,
        const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
        THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& databaseIds)
    {
        TVector<TString> clusters(Reserve(config.ClusterMappingSize()));
        for (auto& cluster: config.GetClusterMapping()) {
            clusters.push_back(cluster.GetName());
        }

        this->SetValidClusters(clusters);

        this->Dispatch(config.GetDefaultSettings());
        for (auto& cluster: config.GetClusterMapping()) {
            this->Dispatch(cluster.GetName(), cluster.GetSettings());

            if (dbResolver) {
                YQL_CLOG(DEBUG, ProviderClickHouse) << "Settings: clusterName = " << cluster.GetName()
                    << ", clusterDbId = "  << cluster.GetId() << ", cluster.GetCluster(): " << cluster.GetCluster() << ", HasCluster: " << (cluster.HasCluster() ? "TRUE" : "FALSE") ;
                databaseIds[std::make_pair(cluster.GetId(), NYql::EDatabaseType::ClickHouse)] =
                    NYql::TDatabaseAuth{cluster.GetCHToken(), /*AddBearer=*/false};
                if (cluster.GetId()) {
                    DbId2Clusters[cluster.GetId()].emplace_back(cluster.GetName());
                    YQL_CLOG(DEBUG, ProviderClickHouse) << "Add dbId: " << cluster.GetId() << " to DbId2Clusters";
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
            Endpoints[cluster.GetName()] = std::make_pair(
                endpoint + ":" + ToString(cluster.GetNativeHostPort()), cluster.GetNativeSecure());

            auto& url = Urls[cluster.GetName()];
            auto& host = std::get<TString>(url);
            auto& scheme = std::get<EHostScheme>(url);
            auto& port = std::get<ui16>(url);
            host = cluster.GetCluster();
            while (host.EndsWith("/"))
                host = host.substr(0u, host.length() - 1u);
            if (host.StartsWith("https://")) {
                scheme = HS_HTTPS;
                host = host.substr(8u);
                port = 443;
            } else {
                scheme = HS_HTTP;
                port = 80;
                if (host.StartsWith("http://")) {
                    host = host.substr(7u);
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

    TClickHouseSettings::TConstPtr Snapshot() const;
    THashMap<TString, TString> Tokens;
    THashMap<TString, std::tuple<TString, EHostScheme, ui16>> Urls;
    THashMap<TString, std::pair<TString, bool>> Endpoints;
    THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
};

} // NYql
