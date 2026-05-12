#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/endpoints/endpoints.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

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
    NYdb::TDeadline Deadline = NYdb::TDeadline::Max();
    std::string TraceParent;

    template <typename TRequestSettings>
    static TRpcRequestSettings Make(const TRequestSettings& settings,
                                    const TEndpointKey& preferredEndpoint = {},
                                    TEndpointPolicy endpointPolicy = TEndpointPolicy::UsePreferredEndpointOptionally) {
        TRpcRequestSettings rpcSettings;
        rpcSettings.TraceId = settings.TraceId_;
        rpcSettings.RequestType = settings.RequestType_;
        rpcSettings.Header = settings.Header_;
        rpcSettings.TraceParent = settings.TraceParent_;
        rpcSettings.PreferredEndpoint = preferredEndpoint;
        rpcSettings.EndpointPolicy = endpointPolicy;
        rpcSettings.UseAuth = true;
        rpcSettings.Deadline = std::min(settings.Deadline_, NYdb::TDeadline::AfterDuration(settings.ClientTimeout_));
        return rpcSettings;
    }

    TRpcRequestSettings& TryUpdateDeadline(const std::optional<TDeadline>& deadline) {
        if (deadline) {
            Deadline = std::min(Deadline, *deadline);
        }
        return *this;
    }
};

} // namespace NYdb
