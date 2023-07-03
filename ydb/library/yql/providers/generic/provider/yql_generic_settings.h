#pragma once

#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    struct TGenericSettings {
        using TConstPtr = std::shared_ptr<const TGenericSettings>;
    };

    struct TGenericConfiguration: public TGenericSettings, public NCommon::TSettingDispatcher {
        using TPtr = TIntrusivePtr<TGenericConfiguration>;

        TGenericConfiguration(){};
        TGenericConfiguration(const TGenericConfiguration&) = delete;

        template <typename TProtoConfig>
        void Init(const TProtoConfig& config,
                  const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
                  NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseIds,
                  const TCredentials::TPtr& credentials)
        {
            TVector<TString> clusterNames(Reserve(config.ClusterMappingSize()));

            for (auto& cluster : config.GetClusterMapping()) {
                clusterNames.push_back(cluster.GetName());
                ClusterNamesToClusterConfigs[cluster.GetName()] = cluster;
            }
            this->SetValidClusters(clusterNames);

            for (const auto& cluster : config.GetClusterMapping()) {
                InitCluster(cluster, dbResolver, databaseIds, credentials);
            }
            this->FreezeDefaults();
        }

    private:
        void InitCluster(const TGenericClusterConfig& cluster,
                         const std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver,
                         NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseIds,
                         const TCredentials::TPtr& credentials) {
            const auto& clusterName = cluster.GetName();
            const auto& databaseId = cluster.GetDatabaseId();
            const auto& endpoint = cluster.GetEndpoint();

            YQL_CLOG(DEBUG, ProviderGeneric)
                << "initialize cluster"
                << ": name = " << clusterName
                << ", database id = " << databaseId
                << ", endpoint = " << endpoint;

            if (dbResolver && databaseId) {
                const auto token = MakeStructuredToken(cluster, credentials);

                databaseIds[std::make_pair(databaseId, NYql::DatabaseType::Generic)] = NYql::TDatabaseAuth{token, /*AddBearer=*/true};

                DatabaseIdsToClusterNames[databaseId].emplace_back(clusterName);
                YQL_CLOG(DEBUG, ProviderGeneric) << "database id '" << databaseId << "' added to mapping";
            }

            // NOTE: Tokens map is filled just because it's required by YQL legacy code.
            // The only reason for provider to store these tokens is
            // to keep compatibility with YQL engine.
            // Real credentials are stored in TGenericClusterConfig.
            Tokens[cluster.GetName()] = " ";
        }

        // Structured tokens are used to access MDB API. They can be constructed from two
        TString MakeStructuredToken(const TGenericClusterConfig& cluster, const TCredentials::TPtr& credentials) {
            TStructuredTokenBuilder b;

            const auto iamToken = credentials->FindCredentialContent("default_" + cluster.name(), "default_generic", cluster.GetToken());
            if (iamToken) {
                return b.SetIAMToken(iamToken).ToJson();
            }

            if (cluster.HasServiceAccountId() && cluster.HasServiceAccountIdSignature()) {
                return b.SetServiceAccountIdAuth(cluster.GetServiceAccountId(), cluster.GetServiceAccountIdSignature()).ToJson();
            }

            ythrow yexception() << "you should either provide IAM Token via credential system or cluster config, "
                                   "or set (ServiceAccountId && ServiceAccountIdSignature) in cluster config";
        }

    public:
        TGenericSettings::TConstPtr Snapshot() const {
            return std::make_shared<const TGenericSettings>(*this);
        }

        bool HasCluster(TStringBuf cluster) const {
            return ValidClusters.contains(cluster);
        }

        THashMap<TString, TString> Tokens;
        THashMap<TString, TGenericClusterConfig> ClusterNamesToClusterConfigs; // cluster name -> cluster config
        THashMap<TString, TVector<TString>> DatabaseIdsToClusterNames;         // database id -> cluster name
    };
}
