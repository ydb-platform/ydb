#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <cstddef>
#include <memory>
#include <mutex>

namespace NMVP::NOIDC {

using TAuthCallbackContextExpirationQueue = TMultiMap<TInstant, TString>;

class TAuthCallbackContextStoreTestAccessor;

struct TAuthCallbackContextRecord {
    TString RequestedAddress;
    TAuthCallbackContextExpirationQueue::iterator ExpirationIt;
};

using TAuthCallbackContextRecordMap = THashMap<TString, TAuthCallbackContextRecord>;

class TAuthCallbackContextStore {
public:
    TAuthCallbackContextStore(TDuration flowLifetime, size_t maxEntries);

    TString Save(const TString& requestedAddress);
    TMaybe<TString> Find(const TString& flowId);

private:
    TMaybe<TString> TryRenewFlow(TStringBuf requestedAddress, TInstant expirationTime);
    TString CreateFlow(TStringBuf requestedAddress, TInstant expirationTime);
    void EraseFlowRecord(TAuthCallbackContextRecordMap::iterator recordIt);
    bool TryEraseOldestFlowRecord();
    void EnforceMaxEntries();
    void CleanupExpired(TInstant now);

private:
    friend class TAuthCallbackContextStoreTestAccessor;

    TDuration FlowLifetime;
    size_t MaxEntries;
    std::mutex Lock;
    TAuthCallbackContextRecordMap FlowRecordsById;
    THashMap<TString, TString> FlowIdByRequestedAddress;
    TAuthCallbackContextExpirationQueue FlowIdsByExpirationTime;
};

using TAuthCallbackContextStorePtr = std::shared_ptr<TAuthCallbackContextStore>;

} // NMVP::NOIDC
