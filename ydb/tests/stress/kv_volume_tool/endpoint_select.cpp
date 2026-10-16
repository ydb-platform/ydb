#include "endpoint_select.h"

#include <util/string/builder.h>

namespace NKvVolumeTool {

bool IsFailover(int code) {
    return code == TryNextAddress || code == TryNextNode;
}

TString FormatHostPort(const TString& host, ui32 port) {
    if (host.find(':') != TString::npos && !host.StartsWith("[")) {
        return TStringBuilder() << "[" << host << "]:" << port;
    }
    return TStringBuilder() << host << ":" << port;
}

TSelectedNode SelectNode(const Ydb::Discovery::EndpointInfo& info, bool useTls) {
    TSelectedNode node;
    auto addAddress = [&](const TString& host) {
        if (host.empty()) {
            return;
        }
        TString address = FormatHostPort(host, info.port());
        for (const auto& existing : node.Addresses) {
            if (existing == address) {
                return;
            }
        }
        node.Addresses.push_back(std::move(address));
    };
    for (const auto& address : info.ip_v4()) {
        addAddress(address);
    }
    for (const auto& address : info.ip_v6()) {
        addAddress(address);
    }
    if (node.Addresses.empty()) {
        addAddress(info.address());
    }

    node.SslTargetNameOverride = info.ssl_target_name_override();
    const bool connectsToAdvertisedHost = node.Addresses.size() == 1 && !info.address().empty() && node.Addresses[0] == FormatHostPort(info.address(), info.port());
    if (useTls && node.SslTargetNameOverride.empty() && !info.address().empty() && !connectsToAdvertisedHost) {
        node.SslTargetNameOverride = info.address();
    }
    return node;
}

bool AdvanceAfterFailover(TFailoverPosition& position, int code, const TVector<size_t>& addressCounts) {
    if (addressCounts.empty() || position.Node >= addressCounts.size()) {
        return false;
    }
    const size_t addresses = addressCounts[position.Node];
    if (code == TryNextAddress && position.Address + 1 < addresses) {
        ++position.Address;
        return true;
    }
    if (position.NodesEntered >= addressCounts.size()) {
        return false;
    }
    position.Node = (position.Node + 1) % addressCounts.size();
    position.Address = 0;
    ++position.NodesEntered;
    return true;
}

} // namespace NKvVolumeTool
