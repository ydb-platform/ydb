#pragma once

#include "public.h"

#include <library/cpp/yt/system/local_host.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

using ::NYT::GetLocalHostName;
using ::NYT::GetLocalHostNameRaw;
using ::NYT::GetLocalYPCluster;
using ::NYT::GetLocalYPClusterRaw;
using ::NYT::SetLocalHostName;

// Returns the loopback address (either IPv4 or IPv6, depending on the configuration).
const std::string& GetLoopbackAddress();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
