#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/endpoints/endpoints.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_control.h>

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
    bool IncludeObservabilityInBuildInfo = false;
    NYdb::TDeadline Deadline = NYdb::TDeadline::Max();
    std::string TraceParent;
    std::shared_ptr<TRequestControl> RequestControl;
    std::shared_ptr<void> RequestLifetime;
    std::string BoundedResponseMethod;

    template <typename TRequestSettings>
    static TRpcRequestSettings Make(const TRequestSettings& settings,
                                    const TEndpointKey& preferredEndpoint = {},
                                    TEndpointPolicy endpointPolicy = TEndpointPolicy::UsePreferredEndpointOptionally) {
        TRpcRequestSettings rpcSettings;
        rpcSettings.TraceId = settings.TraceId_;
        rpcSettings.RequestType = settings.RequestType_;
        rpcSettings.Header = settings.Header_;
        rpcSettings.TraceParent = settings.TraceParent_;
        rpcSettings.RequestControl = settings.RequestControl_;
        rpcSettings.RequestLifetime = settings.RequestLifetime_;
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
