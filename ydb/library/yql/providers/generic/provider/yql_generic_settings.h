#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
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
            TVector<TString> clusterNames(Reserve(config.ClusterMappingSize()));

            for (auto& cluster : config.GetClusterMapping()) {
                clusterNames.push_back(cluster.GetName());
                ClusterConfigs[cluster.GetName()] = cluster;
            }

            this->SetValidClusters(clusterNames);

            // TODO: support data sources other than ClickHouse here
            for (auto& cluster : config.GetClusterMapping()) {
                if (dbResolver) {
                    const auto& databaseID = cluster.GetDatabaseID();
                    YQL_CLOG(DEBUG, ProviderGeneric)
                        << "Settings: clusterName = " << cluster.GetName() << ", cluster cloud id = " << databaseID
                        << ", cluster.GetEndpoint(): " << cluster.GetEndpoint()
                        << ", HasEndpoint: " << (cluster.HasEndpoint() ? "TRUE" : "FALSE");

                    // TODO: recover logic with structured tokens
                    databaseIds[std::make_pair(databaseID, NYql::DatabaseType::Generic)] =
                        NYql::TDatabaseAuth{"", /*AddBearer=*/false};
                    if (databaseID) {
                        DbId2Clusters[databaseID].emplace_back(cluster.GetName());
                        YQL_CLOG(DEBUG, ProviderGeneric) << "Add cluster cloud id: " << databaseID << " to DbId2Clusters";
                    }
                }

                // NOTE: Tokens map is left because of legacy.
                // There are no reasons for provider to store these tokens other than
                // to keep compatibility with YQL engine.
                const auto& basic = cluster.GetCredentials().basic();
                Tokens[cluster.GetName()] = TStringBuilder() << "basic#" << basic.username() << "#" << basic.password();
            }
            this->FreezeDefaults();
        }

        bool HasCluster(TStringBuf cluster) const;

        TGenericSettings::TConstPtr Snapshot() const;
        THashMap<TString, TString> Tokens;
        THashMap<TString, TGenericClusterConfig> ClusterConfigs;
        THashMap<TString, TVector<TString>> DbId2Clusters; // DatabaseId -> ClusterNames
    };

}
