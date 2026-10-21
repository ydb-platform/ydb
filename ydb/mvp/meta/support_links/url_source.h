#pragma once

#include "source.h"

namespace NMVP::NSupportLinks {

void ValidateUrlSourceConfig(const TSupportLinkEntryConfig& config, const TMetaSettings& metaSettings);
std::shared_ptr<ILinkSource> MakeUrlSource(TSupportLinkEntryConfig config, const TMetaSettings& metaSettings);

} // namespace NMVP::NSupportLinks
