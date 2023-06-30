#pragma once

#include <yt/yt/core/logging/log.h>

#include "public.h"

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

//! Choose `portCount` ports among `availablePorts` such that they are free _right now_ and
//! return them. If there are less ports available, helper returns as many ports as possible.
//! This method makes best effort to check that each port is free by making a preliminary bind
//! and immediately releasing it; if this procedure fails, port is skipped.
std::vector<int> AllocateFreePorts(
    int portCount,
    const THashSet<int>& availablePorts,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
