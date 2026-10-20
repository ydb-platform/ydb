#pragma once

#include "credentials.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>
#include <memory>

namespace NFq::NWasmServices {

struct TTvmCredentials {
    ui32 SourceId = 0;
    ui32 DestinationId = 0;
    TString Secret;
};

constexpr ui64 MaxTvmTicketBytes = MaxCredentialBytes;
using ETvmTicketStatus = ECredentialStatus;
using TTvmTicketResult = TCredentialResult;
using ITvmTicketRequest = ICredentialRequest;

// Host-owned and shared between transforms. Implementations must be thread-safe;
// Request/Cancel must not perform blocking I/O on the calling thread.
// Completion may run inline or on any thread, exactly once with Ready/Failed.
// It only publishes a result; the transport resumes processing on its user pool.
// Cancel detaches this waiter; a racing completion must still be safe.
// A real provider owns ticket refresh/coalescing and isolates source, destination
// and credentials. No method, path or user-controlled headers enter this API.
class ITvmTicketProvider {
public:
    virtual ~ITvmTicketProvider() = default;

    virtual bool IsAvailable() const {
        return true;
    }

    using TCompletion = std::function<void(TTvmTicketResult)>;
    virtual std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials& credentials, TInstant deadline, TCompletion completion) = 0;
};

// Explicitly unavailable until the real TVM client/API is supplied. Never issues
// fake tickets and never silently falls back to an unauthenticated request.
std::shared_ptr<ITvmTicketProvider> CreateTvmTicketProvider();

}   // namespace NFq::NWasmServices
