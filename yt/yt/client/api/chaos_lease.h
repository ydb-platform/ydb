#pragma once

#include "prerequisite.h"

#include <yt/yt/client/api/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NChaosClient::TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
