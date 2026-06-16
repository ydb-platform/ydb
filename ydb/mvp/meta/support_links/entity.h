#pragma once

#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/meta/meta_settings.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/vector.h>
#include <optional>

namespace NMVP::NSupportLinks {

enum class EEntityType {
    Cluster,
    Database,
    Node,
    Host,
};

struct TEntityIdentity {
    EEntityType Type = EEntityType::Cluster;
    TString Cluster;
    std::optional<TString> Database;
    std::optional<TString> Node;
    std::optional<TString> Host;
};

inline TStringBuf GetEntityRequestParameterName(EEntityType entityType) {
    switch (entityType) {
        case EEntityType::Cluster:
            return "cluster";
        case EEntityType::Database:
            return "database";
        case EEntityType::Node:
            return "node";
        case EEntityType::Host:
            return "host";
    }
    return {};
}

inline std::optional<TString> ReadOptionalEntityParameter(const NHttp::TUrlParameters& urlParameters, TStringBuf name) {
    if (!urlParameters.Has(name)) {
        return std::nullopt;
    }
    return urlParameters[name];
}

inline std::optional<TString> ReadOptionalEntityParameter(const NHttp::TUrlParameters& urlParameters, EEntityType entityType) {
    return ReadOptionalEntityParameter(urlParameters, GetEntityRequestParameterName(entityType));
}

TEntityIdentity BuildEntityIdentity(EEntityType entityType, const NHttp::TUrlParameters& urlParameters);

inline bool IsIdentityRequestParameter(TStringBuf name) {
    return name == "cluster" || name == "database" || name == "node" || name == "host";
}

TCgiParameters BuildAdditionalRequestParameters(const NHttp::TUrlParameters& urlParameters);
bool TryBuildRequestIdentities(const NHttp::TUrlParameters& urlParameters, TVector<TEntityIdentity>& identities, TString& errorMessage);
const TVector<TSupportLinkEntryConfig>& GetEntityLinkConfigs(const TSupportLinksSettings& settings, EEntityType entityType);

} // namespace NMVP::NSupportLinks
