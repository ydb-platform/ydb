#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NMiniKQL::NAsync {

using TTimePoint = std::chrono::steady_clock::time_point;
using TClock = std::function<TTimePoint()>;
// May run on a provider thread. Must only enqueue a lifetime-safe notification,
// never execute computation inline, block, or throw. The generation lets the
// receiver discard a notification already in flight when Shutdown was called.
using TWakeup = std::function<void(ui64 generation)>;

struct TOperationId {
    ui64 Generation = 0;
    ui64 Sequence = 0;

    bool operator==(const TOperationId&) const = default;
};

enum class EOutcome {
    Success,
    Error,
    Cancelled,
    Timeout,
};

enum class EError {
    None,
    Provider,
    ResponseTooLarge,
    Abandoned,
    StartFailed,
};

struct TResult {
    TOperationId Id;
    EOutcome Outcome = EOutcome::Success;
    EError Error = EError::None;
    TString Payload;
};

// Owned, allocator-independent bytes. Operation identifies a provider-specific
// typed codec; no MiniKQL values, credentials resolver or transport is in this API.
struct TRequest {
    TString Operation;
    TString Payload;
    ui64 MaxResponseBytes = 0;
    std::optional<TTimePoint> Deadline;
};

class ICompletion {
public:
    virtual ~ICompletion() = default;
    virtual TOperationId GetId() const noexcept = 0;
    // Any thread; first terminal result wins. Publish before Release.
    virtual void Complete(TString payload) noexcept = 0;
    virtual void Fail(TString payload) noexcept = 0;
    // Physical cleanup acknowledgement: no resources/input owned by this call
    // remain in the provider. Repeated acknowledgement is harmless. The last
    // completion reference must be kept until cleanup, even after cancellation.
    // Dropping it acknowledges cleanup and fails an otherwise unfinished call.
    virtual void Release() noexcept = 0;
};

class IProvider {
public:
    virtual ~IProvider() = default;
    // Called on the owner execution context; nonblocking. Can publish inline.
    // Keep completion until physical cleanup. A thrown Start becomes StartFailed;
    // if work was already launched, it still has to acknowledge cleanup.
    virtual void Start(TOperationId id, std::shared_ptr<const TRequest> request,
                       std::shared_ptr<ICompletion> completion) = 0;
    // Nonblocking, noexcept, and called at most once per cancellation request.
    // Must not wait for completion, including during provider destruction.
    // Backend state used by outstanding callbacks must outlive them independently
    // of this provider: the context does not retain providers after destruction.
    virtual void Cancel(TOperationId id) noexcept = 0;
};

struct TLimits {
    size_t MaxOperations = 1024;
    ui64 MaxPayloadBytes = 64 * 1024 * 1024;
};

enum class EAdmission {
    Accepted,
    Closed,
    UnknownProvider,
    OperationsLimit,
    BytesLimit,
    DeadlineExceeded,
};

struct TSubmission {
    EAdmission Status;
    TOperationId Id;
};

struct TStats {
    size_t TrackedOperations = 0;
    size_t PendingOperations = 0;
    size_t ReadyResults = 0;
    size_t CleanupPending = 0;
    ui64 ReservedPayloadBytes = 0;
    bool Closed = false;
};

// Task-scoped lifecycle only. Owner methods (except GetStats) are serialized by
// the caller; provider completions may race them on arbitrary threads. No worker,
// timer, polling, actor, MiniKQL graph, batching or engine registration is created.
class TExecutionContext {
public:
    explicit TExecutionContext(TWakeup wakeup, TLimits limits = {},
                               TClock clock = [] { return std::chrono::steady_clock::now(); });
    ~TExecutionContext();
    TExecutionContext(const TExecutionContext&) = delete;
    TExecutionContext& operator=(const TExecutionContext&) = delete;

    void RegisterProvider(TString name, std::shared_ptr<IProvider> provider);
    TSubmission Submit(TStringBuf provider, TRequest request);
    bool Cancel(TOperationId id);

    // Host arms a timer for NextDeadline and calls ExpireDeadlines on its owner
    // context. No polling is required. Clock must be monotonic/thread-safe/noexcept.
    std::optional<TTimePoint> NextDeadline() const;
    size_t ExpireDeadlines();

    // Drains and rearms readiness atomically. After return, new completions or
    // cleanup returning admission credits may request another wakeup. A wakeup
    // can therefore have no ready results; the owner must also retry admission.
    // Ownership/accounting of returned bytes transfers to the consumer; D2/D3 own
    // continuation scheduling and downstream reservations.
    std::vector<TResult> TakeReady();
    // Closes admission, cancels pending work, detaches wakeups, never waits for
    // provider cleanup. Results can still be drained. Idempotent. A notification
    // copied before shutdown can still arrive; its receiver must be lifetime-safe.
    void Shutdown() noexcept;

    ui64 GetGeneration() const noexcept;
    TStats GetStats() const;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // namespace NKikimr::NMiniKQL::NAsync
