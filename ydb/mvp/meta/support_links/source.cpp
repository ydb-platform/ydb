#include "source.h"
#include "grafana_dashboard_source.h"

#include <util/generic/yexception.h>

namespace NMVP {

void ValidateSupportLinksConfig(const TSupportLinksConfig& supportLinks, const TMetaSettings& metaSettings) {
    for (int i = 0; i < supportLinks.GetCluster().size(); ++i) {
        ValidateLinkSourceConfig(supportLinks.GetCluster(i), metaSettings);
    }
    for (int i = 0; i < supportLinks.GetDatabase().size(); ++i) {
        ValidateLinkSourceConfig(supportLinks.GetDatabase(i), metaSettings);
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

    ythrow yexception() << "unsupported support_links source: " << config.GetSource();
}

std::shared_ptr<ILinkSource> MakeLinkSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings) {
    ValidateLinkSourceConfig(config, metaSettings);

    if (config.GetSource() == "grafana/dashboard") {
        return MakeGrafanaDashboardSource(std::move(config), metaSettings);
    }

    ythrow yexception() << "unsupported support_links source: " << config.GetSource();
}

} // namespace NMVP
