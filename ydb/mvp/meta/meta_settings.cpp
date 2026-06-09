#include "meta_settings.h"

#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/core/utils.h>

#include <util/generic/yexception.h>

namespace NMVP {

namespace {

void ValidateGrafanaLoggingLabel(TStringBuf name, TStringBuf value, bool isSet) {
    if (isSet && value.empty()) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "meta.grafana.logging.labels." << name << " must be non-empty";
    }
}

void ApplyGrafanaLoggingSettings(
    const NMvp::NMeta::TMetaConfig::TGrafanaConfig& grafanaConfig,
    TSupportLinksSettings& supportLinksSettings)
{
    if (!grafanaConfig.HasLogging() || !grafanaConfig.GetLogging().HasLabels()) {
        return;
    }

    const auto& labels = grafanaConfig.GetLogging().GetLabels();
    ValidateGrafanaLoggingLabel("cluster", labels.GetCluster(), labels.HasCluster());
    ValidateGrafanaLoggingLabel("database", labels.GetDatabase(), labels.HasDatabase());
    ValidateGrafanaLoggingLabel("node", labels.GetNode(), labels.HasNode());
    ValidateGrafanaLoggingLabel("host", labels.GetHost(), labels.HasHost());

    supportLinksSettings.GrafanaLogging.ClusterLabel = labels.GetCluster();
    supportLinksSettings.GrafanaLogging.DatabaseLabel = labels.GetDatabase();
    supportLinksSettings.GrafanaLogging.NodeLabel = labels.GetNode();
    supportLinksSettings.GrafanaLogging.HostLabel = labels.GetHost();
}

} // namespace

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
        ApplyGrafanaLoggingSettings(config.GetGrafana(), settings.SupportLinks);
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
        settings.SupportLinks.NodeLinks.reserve(supportLinks.GetNode().size());
        for (int i = 0; i < supportLinks.GetNode().size(); ++i) {
            settings.SupportLinks.NodeLinks.push_back(supportLinks.GetNode(i));
        }
        settings.SupportLinks.HostLinks.reserve(supportLinks.GetHost().size());
        for (int i = 0; i < supportLinks.GetHost().size(); ++i) {
            settings.SupportLinks.HostLinks.push_back(supportLinks.GetHost(i));
        }
    }

    return settings;
}

} // namespace NMVP
