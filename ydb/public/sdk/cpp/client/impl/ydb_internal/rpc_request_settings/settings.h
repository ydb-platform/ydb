#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>

namespace NYdb {

struct TRpcRequestSettings {
    TStringType TraceId;
    TStringType RequestType;
    std::vector<std::pair<TStringType, TStringType>> Header;
    enum class TEndpointPolicy {
        UsePreferredEndpoint,
        UseDiscoveryEndpoint // Use single discovery endpoint for request
    } EndpointPolicy = TEndpointPolicy::UsePreferredEndpoint;
    bool UseAuth = true;
    TDuration ClientTimeout;

    template<typename TRequestSettings>
    static TRpcRequestSettings Make(const TRequestSettings& settings) {
        TRpcRequestSettings rpcSettings;
        rpcSettings.TraceId = settings.TraceId_;
        rpcSettings.RequestType = settings.RequestType_;
        rpcSettings.Header = settings.Header_;
        rpcSettings.EndpointPolicy = TEndpointPolicy::UsePreferredEndpoint;
        rpcSettings.UseAuth = true;
        rpcSettings.ClientTimeout = settings.ClientTimeout_;
        return rpcSettings;
    }
};

} // namespace NYdb
