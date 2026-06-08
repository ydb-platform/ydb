#include "source.h"
#include "grafana_dashboard_source.h"
#include "grafana_dashboard_search_source.h"
#include "grafana_logging_source.h"

#include <util/generic/yexception.h>

namespace NMVP {

namespace {

void ValidateSingleSourceOnlyLogging(TStringBuf sectionName, const TSupportLinkEntryConfig& config) {
    if (config.GetSource() != "grafana/logging") {
        ythrow yexception() << "only source=grafana/logging is supported in support_links." << sectionName;
    }
}

} // namespace

void ValidateSupportLinksConfig(const TSupportLinksConfig& supportLinks, const TMetaSettings& metaSettings) {
    for (int i = 0; i < supportLinks.GetCluster().size(); ++i) {
        ValidateLinkSourceConfig(supportLinks.GetCluster(i), metaSettings);
    }
    for (int i = 0; i < supportLinks.GetDatabase().size(); ++i) {
        ValidateLinkSourceConfig(supportLinks.GetDatabase(i), metaSettings);
    }
    for (int i = 0; i < supportLinks.GetNode().size(); ++i) {
        if (supportLinks.GetNode(i).GetSource().empty()) {
            ValidateLinkSourceConfig(supportLinks.GetNode(i), metaSettings);
            continue;
        }
        ValidateSingleSourceOnlyLogging("node", supportLinks.GetNode(i));
        ValidateLinkSourceConfig(supportLinks.GetNode(i), metaSettings);
    }
    for (int i = 0; i < supportLinks.GetHost().size(); ++i) {
        if (supportLinks.GetHost(i).GetSource().empty()) {
            ValidateLinkSourceConfig(supportLinks.GetHost(i), metaSettings);
            continue;
        }
        ValidateSingleSourceOnlyLogging("host", supportLinks.GetHost(i));
        ValidateLinkSourceConfig(supportLinks.GetHost(i), metaSettings);
    }
}

void ValidateLinkSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings) {
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }

    if (config.GetSource() == "grafana/dashboard") {
        ValidateGrafanaDashboardSourceConfig(config, metaSettings);
        return;
    }
    if (config.GetSource() == "grafana/dashboard/search") {
        ValidateGrafanaDashboardSearchSourceConfig(config, metaSettings);
        return;
    }
    if (config.GetSource() == "grafana/logging") {
        ValidateGrafanaLoggingSourceConfig(config, metaSettings);
        return;
    }

    ythrow yexception() << "unsupported support_links source: " << config.GetSource();
}

std::shared_ptr<ILinkSource> MakeLinkSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateLinkSourceConfig(config, metaSettings);

    if (config.GetSource() == "grafana/dashboard") {
        return MakeGrafanaDashboardSource(std::move(config), metaSettings);
    }
    if (config.GetSource() == "grafana/dashboard/search") {
        return MakeGrafanaDashboardSearchSource(std::move(config), metaSettings);
    }
    if (config.GetSource() == "grafana/logging") {
        return MakeGrafanaLoggingSource(std::move(config), metaSettings);
    }

    ythrow yexception() << "unsupported support_links source: " << config.GetSource();
}

} // namespace NMVP
