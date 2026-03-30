#include "meta_settings.h"

#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/core/utils.h>

#include <util/generic/yexception.h>

namespace NMVP {

TMetaSettings BuildMetaSettings(const NMvp::NMeta::TMetaConfig& config, NMvp::EAccessServiceType accessServiceType) {
    TMetaSettings settings;
    settings.MetaApiEndpoint = config.GetMetaApiEndpoint();
    settings.MetaDatabase = config.GetMetaDatabase();
    settings.AccessServiceType = accessServiceType;

    if (settings.MetaApiEndpoint.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "meta.meta_api_endpoint must be specified";
    }
    if (settings.MetaDatabase.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "meta.meta_database must be specified";
    }

    if (config.HasGrafana()) {
        settings.SupportLinks.GrafanaEndpoint = config.GetGrafana().GetEndpoint();
    }

    if (config.HasSupportLinks()) {
        const auto& supportLinks = config.GetSupportLinks();
        ValidateSupportLinksConfig(supportLinks, settings);
        settings.SupportLinks.ClusterLinks.reserve(supportLinks.GetCluster().size());
        for (int i = 0; i < supportLinks.GetCluster().size(); ++i) {
            settings.SupportLinks.ClusterLinks.push_back(supportLinks.GetCluster(i));
        }
        settings.SupportLinks.DatabaseLinks.reserve(supportLinks.GetDatabase().size());
        for (int i = 0; i < supportLinks.GetDatabase().size(); ++i) {
            settings.SupportLinks.DatabaseLinks.push_back(supportLinks.GetDatabase(i));
        }
    }

    return settings;
}

} // namespace NMVP
