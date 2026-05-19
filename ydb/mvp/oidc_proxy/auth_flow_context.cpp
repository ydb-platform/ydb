#include "auth_flow_context.h"
#include "openid_connect.h"

namespace NMVP::NOIDC {

TAuthFlowContextStore::TAuthFlowContextStore(TDuration flowLifetime)
    : FlowLifetime(flowLifetime)
{}

TString TAuthFlowContextStore::Save(const TString& requestedAddress) {
    std::lock_guard<std::mutex> guard(Lock);
    const TInstant now = TInstant::Now();
    CleanupExpired(now);

    const TInstant expirationTime = now + FlowLifetime;

    TString flowId;
    const auto requestedAddressIt = FlowIdByRequestedAddress.find(requestedAddress);
    if (requestedAddressIt != FlowIdByRequestedAddress.end()) {
        flowId = requestedAddressIt->second;
        const auto recordIt = FlowRecordsById.find(flowId);
        if (recordIt != FlowRecordsById.end()) {
            recordIt->second.ExpirationTime = expirationTime;
        } else {
            FlowIdByRequestedAddress.erase(requestedAddressIt);
            flowId.clear();
        }
    }

    if (flowId.empty()) {
        do {
            flowId = GenerateRandomBase64(32);
        } while (FlowRecordsById.contains(flowId));

        FlowRecordsById[flowId] = {
            .RequestedAddress = requestedAddress,
            .ExpirationTime = expirationTime,
        };
        FlowIdByRequestedAddress[requestedAddress] = flowId;
    }

    FlowIdsByExpirationTime.emplace(expirationTime, flowId);
    return flowId;
}

TMaybe<TString> TAuthFlowContextStore::Find(const TString& flowId) {
    std::lock_guard<std::mutex> guard(Lock);
    CleanupExpired(TInstant::Now());

    const auto it = FlowRecordsById.find(flowId);
    if (it == FlowRecordsById.end()) {
        return Nothing();
    }

    return it->second.RequestedAddress;
}

void TAuthFlowContextStore::CleanupExpired(TInstant now) {
    while (!FlowIdsByExpirationTime.empty()) {
        const auto it = FlowIdsByExpirationTime.begin();
        if (it->first > now) {
            break;
        }

        const auto recordIt = FlowRecordsById.find(it->second);
        if (recordIt == FlowRecordsById.end() || recordIt->second.ExpirationTime != it->first) {
            FlowIdsByExpirationTime.erase(it);
            continue;
        }

        FlowIdByRequestedAddress.erase(recordIt->second.RequestedAddress);
        FlowRecordsById.erase(recordIt);
        FlowIdsByExpirationTime.erase(it);
    }
}

} // NMVP::NOIDC
