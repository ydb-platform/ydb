#pragma once

#include "source.h"

namespace NMVP::NSupportLinks {

void ValidateGrafanaDashboardSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeGrafanaDashboardSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
