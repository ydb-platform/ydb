#include "yql_generic_cluster_config.h"
#include "yql_generic_settings.h"
#include "yql_generic_utils.h"

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

    const TString TGenericSettings::TDefault::DateTimeFormat = "string";

    TGenericConfiguration::TGenericConfiguration() {
        REGISTER_SETTING(*this, UsePredicatePushdown);
        REGISTER_SETTING(*this, DateTimeFormat);
    }

    void TGenericConfiguration::Init(const NYql::TGenericGatewayConfig& gatewayConfig,
                                     const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                                     NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth,
                                     const TCredentials::TPtr& credentials)
    {
        Dispatch(gatewayConfig.GetDefaultSettings());

        DescribeTableTimeout = gatewayConfig.HasDescribeTableTimeoutSeconds() ? 
                               TDuration::Seconds(gatewayConfig.GetDescribeTableTimeoutSeconds()) :
                               TDuration::Seconds(60);
    
        for (const auto& cluster : gatewayConfig.GetClusterMapping()) {
            AddCluster(cluster, databaseResolver, databaseAuth, credentials);
        }

        // TODO: check if it's necessary
        FreezeDefaults();
    }

    void TGenericConfiguration::AddCluster(const TGenericClusterConfig& clusterConfig,
                                           const std::shared_ptr<NYql::IDatabaseAsyncResolver> databaseResolver,
                                           NYql::IDatabaseAsyncResolver::TDatabaseAuthMap& databaseAuth,
                                           const TCredentials::TPtr& credentials) {
        ValidateGenericClusterConfig(clusterConfig, "TGenericConfiguration::AddCluster");

        YQL_CLOG(INFO, ProviderGeneric) << "GenericConfiguration::AddCluster: " << DumpGenericClusterConfig(clusterConfig);

        const auto& clusterName = clusterConfig.GetName();
        const auto& databaseId = clusterConfig.GetDatabaseId();

        if (databaseId) {
            if (!databaseResolver) {
                ythrow yexception() << "You're trying to access managed database, but database resolver is not configured.";
            }

            const auto token = MakeStructuredToken(clusterConfig, credentials);

            databaseAuth[std::make_pair(databaseId, DatabaseTypeFromDataSourceKind(clusterConfig.GetKind()))] =
                NYql::TDatabaseAuth{
                    .StructuredToken = token,
                    .AddBearerToToken = true,
                    .UseTls = clusterConfig.GetUseSsl(),
                    .Protocol = clusterConfig.GetProtocol()};

            DatabaseIdsToClusterNames[databaseId].emplace_back(clusterName);
            YQL_CLOG(DEBUG, ProviderGeneric) << "database id '" << databaseId << "' added to mapping";
        }

        // NOTE: Tokens map is filled just because it's required by DQ/KQP.
        // The only reason for provider to store these tokens is
        // to keep compatibility with these engines.
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
        this->AddValidCluster(clusterConfig.GetName());
    }

    // Structured tokens are used to access MDB API. They can be constructed either from IAM tokens, or from SA credentials.
    TString TGenericConfiguration::MakeStructuredToken(const TGenericClusterConfig& cluster, const TCredentials::TPtr& credentials) const {
        TStructuredTokenBuilder b;

        const auto iamToken = credentials->FindCredentialContent(
            "default_" + cluster.name(),
            "default_generic",
            cluster.GetToken());
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
        return GetValidClusters().contains(cluster);
    }

} // namespace NYql
