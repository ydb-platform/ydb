#include "entity.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

TEntityIdentity BuildEntityIdentity(EEntityType entityType, const NHttp::TUrlParameters& urlParameters) {
    TEntityIdentity entityIdentity{
        .Type = entityType,
        .Cluster = urlParameters[GetEntityRequestParameterName(EEntityType::Cluster)],
    };

    switch (entityType) {
        case EEntityType::Cluster:
            break;
        case EEntityType::Database:
            entityIdentity.Database = ReadOptionalEntityParameter(urlParameters, entityType);
            break;
        case EEntityType::Node:
            entityIdentity.Node = ReadOptionalEntityParameter(urlParameters, entityType);
            break;
        case EEntityType::Host:
            entityIdentity.Host = ReadOptionalEntityParameter(urlParameters, entityType);
            break;
    }

    return entityIdentity;
}

TCgiParameters BuildAdditionalRequestParameters(const NHttp::TUrlParameters& urlParameters) {
    TCgiParameters additionalRequestParams;
    for (const auto& [name, _] : urlParameters.Parameters) {
        if (IsIdentityRequestParameter(name)) {
            continue;
        }
        additionalRequestParams.InsertUnescaped(name, urlParameters[name]);
    }
    return additionalRequestParams;
}

bool TryBuildRequestIdentities(const NHttp::TUrlParameters& urlParameters, TVector<TEntityIdentity>& identities, TString& errorMessage) {
    const TString cluster = urlParameters[GetEntityRequestParameterName(EEntityType::Cluster)];
    const auto database = ReadOptionalEntityParameter(urlParameters, EEntityType::Database);
    const auto node = ReadOptionalEntityParameter(urlParameters, EEntityType::Node);
    const auto host = ReadOptionalEntityParameter(urlParameters, EEntityType::Host);

    if (cluster.empty()) {
        errorMessage = "Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'; node requires 'cluster' and 'node'; host requires 'cluster' and 'host'.";
        return false;
    }
    if ((node || host) && database) {
        errorMessage = "Invalid identity parameters. Parameter 'database' must not be specified together with 'node' or 'host'.";
        return false;
    }

    TVector<TEntityIdentity> result;
    if (node) {
        if (node->empty()) {
            errorMessage = "Invalid identity parameters. Parameter 'node' must not be empty.";
            return false;
        }
        result.push_back(TEntityIdentity{.Type = EEntityType::Node, .Cluster = cluster, .Node = node});
    }
    if (host) {
        if (host->empty()) {
            errorMessage = "Invalid identity parameters. Parameter 'host' must not be empty.";
            return false;
        }
        result.push_back(TEntityIdentity{.Type = EEntityType::Host, .Cluster = cluster, .Host = host});
    }
    if (database && !database->empty()) {
        result.push_back(TEntityIdentity{.Type = EEntityType::Database, .Cluster = cluster, .Database = database});
    }
    if (result.empty()) {
        result.push_back(TEntityIdentity{.Type = EEntityType::Cluster, .Cluster = cluster});
    }

    identities = std::move(result);
    return true;
}

const TVector<TSupportLinkEntryConfig>& GetEntityLinkConfigs(const TSupportLinksSettings& settings, EEntityType entityType) {
    switch (entityType) {
        case EEntityType::Cluster:
            return settings.ClusterLinks;
        case EEntityType::Database:
            return settings.DatabaseLinks;
        case EEntityType::Node:
            return settings.NodeLinks;
        case EEntityType::Host:
            return settings.HostLinks;
    }
    ythrow yexception() << "unsupported support links entity type";
}

} // namespace NMVP::NSupportLinks
