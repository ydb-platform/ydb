#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

//! Returns an upper bound of the memory a heavy command may hold in its reader or writer buffers,
//! derived from |config| and the per-request config overrides in |parameters|.
//! Returns null for commands without a known estimate.
std::optional<i64> EstimateHeavyCommandMemoryUsage(
    const std::string& commandName,
    const TDriverConfigPtr& config,
    const NYTree::IMapNodePtr& parameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
