#pragma once

#include "source.h"

namespace NMVP::NSupportLinks {

void ValidateGrafanaLoggingSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeGrafanaLoggingSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
