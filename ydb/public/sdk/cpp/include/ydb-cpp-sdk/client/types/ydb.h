#pragma once

#include "fwd.h"
#include "status_codes.h"

namespace NYdb::inline V3 {

enum class EDiscoveryMode {
    //! Block in ctor (driver or client if database and/or auth token is overridden by client settings)
    //! until we get list of endpoints, if list of endpoints become empty during executing requests
    //! corresponding error will be returned.
    //! Note: Even in Sync mode SDK will perform lazy async update of endpoints list
    Sync,
    //! Do not block in ctor to get endpoint list.
    //! If list of endpoints is empty in time of request (or becomes empty during execution) the request will be pending until
    //! we got endpoint list. The error will be returned if the endpoint list
    //! is empty and discovery failed
    //! This method is a bit more "user friendly" but can produce additional hidden latency
    Async,
    //! Do not perform discovery.
    //! This option disables database discovery and allow to use user provided endpoint for grpc connections
    Off
};

enum class EBalancingPolicy {
    //! Use all available cluster nodes regardless datacenter locality
    UseAllNodes,
    //! Use preferable location,
    //! params is a name of location (VLA, MAN), if params is empty local datacenter is used
    UsePreferableLocation
};

} // namespace NYdb
