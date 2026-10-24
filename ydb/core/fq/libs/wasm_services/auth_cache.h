#pragma once

#include "credentials.h"

#include <ydb/services/udf_store/wasm/async_runtime/runtime.h>

namespace NFq::NWasmServices {

struct TAuthCacheLimits {
    ui64 MaxEntries = 64;
    ui64 MaxWaiters = 128;
    ui64 MaxBytes = 2 << 20;
    TDuration RefreshTimeout = TDuration::Seconds(30);
    TDuration ExpiryMargin = TDuration::MilliSeconds(100);
};

struct TAuthRefreshResult {
    TCredentialResult Credential;
    TInstant ExpiresAt;
};

// Exactly one instance belongs to a transform compartment. Keys are created by
// the host and include provider, connection access context and credential refs.
class TAuthCache {
public:
    using TCancel = NKikimr::NUdfStore::NWasm::NAsync::ITransport::TCancel;
    using TCompletion = std::function<void(TAuthRefreshResult)>;
    using TLoader = std::function<TCancel(const TString&, TInstant, TCompletion)>;
    explicit TAuthCache(TLoader loader, TAuthCacheLimits limits = {});
    ~TAuthCache();
    std::shared_ptr<ICredentialRequest> Request(const TString& key, TInstant deadline, ICredentialProvider::TCompletion completion);
    void Expire(TInstant now);
    TInstant NextDeadline() const;
    void Close();

    struct TStats {
        ui64 Entries, Waiters, Bytes;
    };

    TStats Stats() const;

private:
    struct TState;
    std::shared_ptr<TState> State_;
};
}   // namespace NFq::NWasmServices
