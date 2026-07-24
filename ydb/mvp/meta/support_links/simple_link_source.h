#pragma once

#include "source.h"

namespace NMVP::NSupportLinks {

void ValidateSimpleLinkSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeSimpleLinkSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
