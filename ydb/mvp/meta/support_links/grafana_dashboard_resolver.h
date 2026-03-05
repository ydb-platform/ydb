#pragma once

#include "common.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

inline void ValidateGrafanaDashboardResolverConfig(const TResolverValidationContext& context)
{
    if (context.LinkConfig.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << context.LinkConfig.GetSource();
    }
    if (!IsAbsoluteUrl(context.LinkConfig.GetUrl()) && context.GrafanaConfig.Endpoint.empty()) {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
}

} // namespace NMVP::NSupportLinks
