#include "entity.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

TEntityIdentity BuildEntityIdentity(EEntityType entityType, const NHttp::TUrlParameters& urlParameters) {
    TEntityIdentity entityIdentity{
        .Type = entityType,
        .Cluster = urlParameters[GetEntityRequestParameterName(EEntityType::Cluster)],
    };

    if (entityType == EEntityType::Database) {
        entityIdentity.Database = ReadOptionalEntityParameter(urlParameters, entityType);
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

    if (cluster.empty()) {
        errorMessage = "Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'.";
        return false;
    }

    TVector<TEntityIdentity> result;
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
    }
    ythrow yexception() << "unsupported support links entity type";
}

} // namespace NMVP::NSupportLinks
