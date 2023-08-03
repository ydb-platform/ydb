#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>

namespace NYdb {

struct TRpcRequestSettings {
    TStringType TraceId;
    TStringType RequestType;
    std::vector<std::pair<TStringType, TStringType>> Header;
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
