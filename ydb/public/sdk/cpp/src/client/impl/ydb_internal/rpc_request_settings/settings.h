#pragma once

#include <src/client/impl/ydb_endpoints/endpoints.h>
#include <src/client/impl/ydb_internal/internal_header.h>

namespace NYdb::inline Dev {

struct TRpcRequestSettings {
    std::string TraceId;
    std::string RequestType;
    std::vector<std::pair<std::string, std::string>> Header;
    TEndpointKey PreferredEndpoint = {};
    enum class TEndpointPolicy {
        UsePreferredEndpointOptionally, // Try to use the preferred endpoint
        UsePreferredEndpointStrictly,   // Use only the preferred endpoint
        UseDiscoveryEndpoint            // Use single discovery endpoint
    } EndpointPolicy = TEndpointPolicy::UsePreferredEndpointOptionally;
    bool UseAuth = true;
    TDuration ClientTimeout;

    template <typename TRequestSettings>
    static TRpcRequestSettings Make(const TRequestSettings& settings, const TEndpointKey& preferredEndpoint = {}, TEndpointPolicy endpointPolicy = TEndpointPolicy::UsePreferredEndpointOptionally) {
        TRpcRequestSettings rpcSettings;
        rpcSettings.TraceId = settings.TraceId_;
        rpcSettings.RequestType = settings.RequestType_;
        rpcSettings.Header = settings.Header_;
        rpcSettings.PreferredEndpoint = preferredEndpoint;
        rpcSettings.EndpointPolicy = endpointPolicy;
        rpcSettings.UseAuth = true;
        rpcSettings.ClientTimeout = settings.ClientTimeout_;
        return rpcSettings;
    }
};

} // namespace NYdb
