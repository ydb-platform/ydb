#include "endpoint_select.h"

#include <util/string/builder.h>
#include <util/system/types.h>

namespace NKvVolumeTool {

namespace {

TString FormatHostPort(const TString& host, ui32 port) {
    if (host.find(':') != TString::npos && !host.StartsWith("[")) {
        return TStringBuilder() << "[" << host << "]:" << port;
    }
    return TStringBuilder() << host << ":" << port;
}

} // namespace

TVector<TEndpointCandidate> SelectEndpoints(const Ydb::Discovery::EndpointInfo& info, bool useTls) {
    TVector<TString> hosts;
    auto addHost = [&](const TString& host) {
        if (host.empty()) {
            return;
        }
        TString address = FormatHostPort(host, info.port());
        for (const auto& existing : hosts) {
            if (existing == address) {
                return;
            }
        }
        hosts.push_back(std::move(address));
    };
    for (const auto& address : info.ip_v4()) {
        addHost(address);
    }
    for (const auto& address : info.ip_v6()) {
        addHost(address);
    }
    if (hosts.empty()) {
        addHost(info.address());
    }

    TString sslTargetNameOverride = info.ssl_target_name_override();
    const bool connectsToAdvertisedHost = hosts.size() == 1 && !info.address().empty() && hosts[0] == FormatHostPort(info.address(), info.port());
    if (useTls && sslTargetNameOverride.empty() && !info.address().empty() && !connectsToAdvertisedHost) {
        sslTargetNameOverride = info.address();
    }

    TVector<TEndpointCandidate> endpoints;
    endpoints.reserve(hosts.size());
    for (auto& host : hosts) {
        endpoints.push_back({std::move(host), sslTargetNameOverride});
    }
    return endpoints;
}

} // namespace NKvVolumeTool
