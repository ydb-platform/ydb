#pragma once

#include "source.h"

namespace NMVP::NSupportLinks {

void ValidateGrafanaDashboardSearchSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeGrafanaDashboardSearchSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
