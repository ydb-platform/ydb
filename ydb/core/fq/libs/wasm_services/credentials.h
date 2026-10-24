#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <functional>
#include <memory>

namespace NFq::NWasmServices {
inline constexpr ui64 MaxCredentialBytes = 16384;
enum class ECredentialStatus {
    Ready,
    Failed
};

struct TCredentialResult {
    ECredentialStatus Status = ECredentialStatus::Failed;
    TString Ticket;
};

class ICredentialRequest {
public:
    virtual ~ICredentialRequest() = default;
    virtual void Cancel() noexcept = 0;
};

class ICredentialProvider {
public:
    using TCompletion = std::function<void(TCredentialResult)>;
    virtual ~ICredentialProvider() = default;
    // Nonblocking; cancellation detaches only this waiter. Completion can race
    // cancellation, run inline or on another thread. Never enter WASM here.
    virtual std::shared_ptr<ICredentialRequest> Request(TInstant deadline, TCompletion completion) = 0;
};
}   // namespace NFq::NWasmServices
