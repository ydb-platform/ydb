#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <memory>

namespace NFq::NWasmServices {

struct TTvmCredentials {
    ui32 SourceId = 0;
    ui32 DestinationId = 0;
    TString Secret;
};

constexpr ui64 MaxTvmTicketBytes = 16384;

enum class ETvmTicketStatus { Pending, Ready, Failed };

struct TTvmTicketResult {
    ETvmTicketStatus Status = ETvmTicketStatus::Pending;
    TString Ticket;
};

// Nonblocking acquisition. Cancel drops this waiter, not other callers waiting
// for the same ticket. Destruction must also detach the waiter without blocking.
class ITvmTicketRequest {
public:
    virtual ~ITvmTicketRequest() = default;
    virtual TTvmTicketResult Poll() = 0;
    virtual void Cancel() noexcept = 0;
};

// Host-owned and shared between transforms. Implementations must be thread-safe;
// Request/Poll/Cancel must not perform blocking I/O on the calling thread.
// A real provider owns ticket refresh/coalescing and isolates source, destination
// and credentials. No method, path or user-controlled headers enter this API.
class ITvmTicketProvider {
public:
    virtual ~ITvmTicketProvider() = default;
    virtual bool IsAvailable() const { return true; }
    virtual std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials& credentials, TInstant deadline) = 0;
};

// Explicitly unavailable until the real TVM client/API is supplied. Never issues
// fake tickets and never silently falls back to an unauthenticated request.
std::shared_ptr<ITvmTicketProvider> CreateTvmTicketProvider();

} // namespace NFq::NWasmServices
