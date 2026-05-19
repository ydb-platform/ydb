#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <memory>
#include <mutex>

namespace NMVP::NOIDC {

struct TAuthFlowRecord {
    TString RequestedAddress;
    TInstant ExpirationTime;
};

class TAuthFlowContextStore {
public:
    explicit TAuthFlowContextStore(TDuration flowLifetime);

    TString Save(const TString& requestedAddress);
    TMaybe<TString> Find(const TString& flowId);

private:
    void CleanupExpired(TInstant now);

private:
    TDuration FlowLifetime;
    std::mutex Lock;
    THashMap<TString, TAuthFlowRecord> FlowRecordsById;
    THashMap<TString, TString> FlowIdByRequestedAddress;
    TMultiMap<TInstant, TString> FlowIdsByExpirationTime;
};

using TAuthFlowContextStorePtr = std::shared_ptr<TAuthFlowContextStore>;

} // NMVP::NOIDC
