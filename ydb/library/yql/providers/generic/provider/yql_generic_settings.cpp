#include "yql_generic_settings.h"

#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    void TGenericConfiguration::Init(const NYql::TGenericGatewayConfig& gatewayConfig,
                                     const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                                     NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth,
                                     const TCredentials::TPtr& credentials)
    {
        for (const auto& cluster : gatewayConfig.GetClusterMapping()) {
            AddCluster(cluster, databaseResolver, databaseAuth, credentials);
        }

        // TODO: check if it's necessary
        this->FreezeDefaults();
    }

    void TGenericConfiguration::AddCluster(const TGenericClusterConfig& clusterConfig,
                                           const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                                           NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth,
                                           const TCredentials::TPtr& credentials) {
        const auto& clusterName = clusterConfig.GetName();
        const auto& databaseId = clusterConfig.GetDatabaseId();
        const auto& endpoint = clusterConfig.GetEndpoint();

        YQL_CLOG(DEBUG, ProviderGeneric)
            << "add cluster"
            << ": name = " << clusterName
            << ", database id = " << databaseId
            << ", endpoint = " << endpoint;

        // if cluster FQDN's is not known
        if (databaseResolver && databaseId) {
            const auto token = MakeStructuredToken(clusterConfig, credentials);

            databaseAuth[std::make_pair(databaseId, DataSourceKindToDatabaseType(clusterConfig.GetKind()))] =
                NYql::TDatabaseAuth{token, /*AddBearer=*/true};

            DatabaseIdsToClusterNames[databaseId].emplace_back(clusterName);
            YQL_CLOG(DEBUG, ProviderGeneric) << "database id '" << databaseId << "' added to mapping";
        }

        // NOTE: Tokens map is filled just because it's required by YQL engine code.
        // The only reason for provider to store these tokens is
        // to keep compatibility with YQL engine.
        // Real credentials are stored in TGenericClusterConfig.
        Tokens[clusterConfig.GetName()] =
            TStructuredTokenBuilder()
                .SetBasicAuth(
                    clusterConfig.GetCredentials().basic().username(),
                    clusterConfig.GetCredentials().basic().password())
                .ToJson();

        // preserve cluster config entirely for the further use
        ClusterNamesToClusterConfigs[clusterName] = clusterConfig;

        // Add cluster to the list of valid clusters
        this->ValidClusters.insert(clusterConfig.GetName());
    }

    // Structured tokens are used to access MDB API. They can be constructed either from IAM tokens, or from SA credentials.
    TString TGenericConfiguration::MakeStructuredToken(const TGenericClusterConfig& cluster, const TCredentials::TPtr& credentials) const {
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

    TGenericSettings::TConstPtr TGenericConfiguration::Snapshot() const {
        return std::make_shared<const TGenericSettings>(*this);
    }

    bool TGenericConfiguration::HasCluster(TStringBuf cluster) const {
        return ValidClusters.contains(cluster);
    }

}