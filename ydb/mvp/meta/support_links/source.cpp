#include "source.h"

#include "grafana_dashboard_source.h"
#include "grafana_dashboard_search_source.h"

#include <util/generic/yexception.h>

namespace NMVP {

std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config, const TMetaSettings& metaSettings) {
    (void)place;
    if (config.GetSource().empty()) {
        ythrow yexception() << "source is required";
    }
    ythrow yexception() << "unsupported source=" << config.GetSource();
}

std::shared_ptr<ILinkSource> MakeLinkSource(size_t place, TSupportLinkEntry config) {
    Y_ABORT_UNLESS(InstanceMVP);
    return MakeLinkSource(place, std::move(config), InstanceMVP->MetaSettings);
}

} // namespace NMVP
