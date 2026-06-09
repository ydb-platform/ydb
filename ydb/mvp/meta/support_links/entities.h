#pragma once

#include <ydb/mvp/meta/meta_settings.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NMVP {

enum class ESupportLinksEntityType {
    Cluster,
    Database,
    Node,
    Host,
};

inline const TVector<ESupportLinksEntityType>& GetAllSupportLinksEntityTypes() {
    static const TVector<ESupportLinksEntityType> entityTypes = {
        ESupportLinksEntityType::Cluster,
        ESupportLinksEntityType::Database,
        ESupportLinksEntityType::Node,
        ESupportLinksEntityType::Host,
    };
    return entityTypes;
}

inline TStringBuf GetSupportLinksEntityName(ESupportLinksEntityType entityType) {
    switch (entityType) {
        case ESupportLinksEntityType::Cluster:
            return "cluster";
        case ESupportLinksEntityType::Database:
            return "database";
        case ESupportLinksEntityType::Node:
            return "node";
        case ESupportLinksEntityType::Host:
            return "host";
    }
    ythrow yexception() << "unsupported support links entity type";
}

inline const TVector<TSupportLinkEntryConfig>& GetSupportLinksEntityConfigs(
    const TSupportLinksSettings& settings,
    ESupportLinksEntityType entityType)
{
    switch (entityType) {
        case ESupportLinksEntityType::Cluster:
            return settings.ClusterLinks;
        case ESupportLinksEntityType::Database:
            return settings.DatabaseLinks;
        case ESupportLinksEntityType::Node:
            return settings.NodeLinks;
        case ESupportLinksEntityType::Host:
            return settings.HostLinks;
    }
    ythrow yexception() << "unsupported support links entity type";
}

} // namespace NMVP
