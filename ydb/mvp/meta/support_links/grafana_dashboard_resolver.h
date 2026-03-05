#pragma once

#include "common.h"

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

inline void ValidateGrafanaDashboardResolverConfig(const TResolverValidationContext& context)
{
    if (context.LinkConfig.Url.empty()) {
        ythrow yexception() << context.Where << ": url is required for source=" << context.LinkConfig.Source;
    }
    if (!IsAbsoluteUrl(context.LinkConfig.Url) && context.GrafanaConfig.Endpoint.empty()) {
        ythrow yexception() << context.Where << ": grafana.endpoint is required for relative url";
    }
}

} // namespace NMVP::NSupportLinks
