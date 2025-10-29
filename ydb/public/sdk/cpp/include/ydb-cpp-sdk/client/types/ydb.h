#pragma once

#include "fwd.h"
#include "status_codes.h"

#include <memory>
#include <optional>
#include <string>


namespace NYdb::inline Dev {

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

//! @deprecated Use TBalancingPolicy instead
enum class EBalancingPolicy {
    //! Use all available cluster nodes regardless datacenter locality
    UseAllNodes,
    //! Use preferable location,
    //! params is a name of location (VLA, MAN), if params is empty local datacenter is used
    UsePreferableLocation,
};

enum EPileState {
    UNSPECIFIED = 0 /* "unspecified" */,
    PRIMARY = 1 /* "primary" */,
    PROMOTED = 2 /* "promoted" */,
    SYNCHRONIZED = 3 /* "synchronized" */,
    NOT_SYNCHRONIZED = 4 /* "not_synchronized" */,
    SUSPENDED = 5 /* "suspended" */,
    DISCONNECTED = 6 /* "disconnected" */
};

class TBalancingPolicy {
    friend class TDriverConfig;
    friend class TDriver;
public:
    //! Use preferable location,
    //! location is a name of datacenter (VLA, MAN), if location is nullopt local datacenter is used
    static TBalancingPolicy UsePreferableLocation(const std::optional<std::string>& location = {});

    //! Use all available cluster nodes regardless datacenter locality
    static TBalancingPolicy UseAllNodes();

    //! EXPERIMENTAL
    //! Use pile with preferable state
    static TBalancingPolicy UsePreferablePileState(EPileState pileState = EPileState::PRIMARY);

    TBalancingPolicy(const TBalancingPolicy&) = delete;
    TBalancingPolicy(TBalancingPolicy&&) = default;
    TBalancingPolicy& operator=(const TBalancingPolicy&) = delete;
    TBalancingPolicy& operator=(TBalancingPolicy&&) = default;

    ~TBalancingPolicy();

    class TImpl;
private:
    TBalancingPolicy(std::unique_ptr<TImpl>&& impl);

    // For deprecated EBalancingPolicy
    TBalancingPolicy(EBalancingPolicy policy, const std::string& params);

    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb
