#include "auth_callback_context_store.h"
#include "openid_connect.h"

namespace NMVP::NOIDC {

TAuthCallbackContextStore::TAuthCallbackContextStore(TDuration flowLifetime, size_t maxEntries)
    : FlowLifetime(flowLifetime)
    , MaxEntries(maxEntries)
{}

TString TAuthCallbackContextStore::Save(const TString& requestedAddress) {
    std::lock_guard<std::mutex> guard(Lock);
    const TInstant now = TInstant::Now();
    CleanupExpired(now);

    const TInstant expirationTime = now + FlowLifetime;
    const TMaybe<TString> flowId = TryRenewFlow(requestedAddress, expirationTime);
    if (flowId) {
        return *flowId;
    }

    EnforceMaxEntries();
    return CreateFlow(requestedAddress, expirationTime);
}

TMaybe<TString> TAuthCallbackContextStore::Find(const TString& flowId) {
    std::lock_guard<std::mutex> guard(Lock);
    CleanupExpired(TInstant::Now());

    const auto it = FlowRecordsById.find(flowId);
    if (it == FlowRecordsById.end()) {
        return Nothing();
    }

    return it->second.RequestedAddress;
}

TMaybe<TString> TAuthCallbackContextStore::TryRenewFlow(TStringBuf requestedAddress, TInstant expirationTime) {
    const auto requestedAddressIt = FlowIdByRequestedAddress.find(requestedAddress);
    if (requestedAddressIt == FlowIdByRequestedAddress.end()) {
        return Nothing();
    }

    auto recordIt = FlowRecordsById.find(requestedAddressIt->second);
    if (recordIt != FlowRecordsById.end()) {
        FlowIdsByExpirationTime.erase(recordIt->second.ExpirationIt);
        recordIt->second.ExpirationIt = FlowIdsByExpirationTime.emplace(expirationTime, recordIt->first);
        return recordIt->first;
    }

    FlowIdByRequestedAddress.erase(requestedAddressIt);
    return Nothing();
}

TString TAuthCallbackContextStore::CreateFlow(TStringBuf requestedAddress, TInstant expirationTime) {
    TString flowId;
    do {
        flowId = GenerateRandomBase64();
    } while (FlowRecordsById.contains(flowId));

    const TString requestedAddressString(requestedAddress);
    const auto expirationIt = FlowIdsByExpirationTime.emplace(expirationTime, flowId);
    FlowRecordsById.emplace(flowId, TAuthCallbackContextRecord{
        .RequestedAddress = requestedAddressString,
        .ExpirationIt = expirationIt,
    });
    FlowIdByRequestedAddress[requestedAddressString] = flowId;
    return flowId;
}

void TAuthCallbackContextStore::EraseFlowRecord(TAuthCallbackContextRecordMap::iterator recordIt) {
    FlowIdsByExpirationTime.erase(recordIt->second.ExpirationIt);
    FlowIdByRequestedAddress.erase(recordIt->second.RequestedAddress);
    FlowRecordsById.erase(recordIt);
}

bool TAuthCallbackContextStore::TryEraseOldestFlowRecord() {
    while (!FlowIdsByExpirationTime.empty()) {
        const auto expirationIt = FlowIdsByExpirationTime.begin();
        const auto recordIt = FlowRecordsById.find(expirationIt->second);
        if (recordIt == FlowRecordsById.end() || recordIt->second.ExpirationIt != expirationIt) {
            FlowIdsByExpirationTime.erase(expirationIt);
            continue;
        }

        EraseFlowRecord(recordIt);
        return true;
    }

    return false;
}

void TAuthCallbackContextStore::EnforceMaxEntries() {
    while (FlowRecordsById.size() >= MaxEntries && TryEraseOldestFlowRecord()) {
    }
}

void TAuthCallbackContextStore::CleanupExpired(TInstant now) {
    while (!FlowIdsByExpirationTime.empty() && FlowIdsByExpirationTime.begin()->first <= now) {
        if (!TryEraseOldestFlowRecord()) {
            break;
        }
    }
}

} // NMVP::NOIDC
