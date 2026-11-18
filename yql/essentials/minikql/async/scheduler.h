#pragma once

#include "async.h"
#include "resources.h"

namespace NKikimr::NMiniKQL::NAsync {

struct TBatchItem {
    TOperationId Id;
    std::shared_ptr<const TRequest> Request;
    std::shared_ptr<ICompletion> Completion;
};
// Explicit opt-in. The opaque key must include all security/tenant/codec and
// cancellation compatibility. Core additionally separates registration, operation
// and exact deadline. Each item retains its own result and cancellation identity.
class IBatchProvider: public IProvider {
public:
    virtual TString GetBatchKey(const TRequest& request) const = 0;
    virtual size_t GetMaxBatchSize() const noexcept = 0;
    virtual void StartBatch(std::vector<TBatchItem> items) = 0;
    void Start(TOperationId id, std::shared_ptr<const TRequest> request, std::shared_ptr<ICompletion> completion) override {
        StartBatch({{id, std::move(request), std::move(completion)}});
    }
};

enum class EEventKind {
    Submitted,
    Dispatched,
    Completed
};
struct TExecutionEvent {
    EEventKind Kind;
    TOperationId Id;
    EOutcome Outcome = EOutcome::Success;
    ui64 QueueWaitUs = 0;
    ui64 ExecutionWaitUs = 0;
};
// Owner-only, nonthrowing, no payloads/keys/credentials. Transport metrics remain
// provider-owned; a generic dispatch does not assert a physical network send.
using TTraceHook = std::function<void(const TExecutionEvent&)>;
struct TSchedulerSettings {
    size_t MaxInFlight = 64;
    size_t MaxBatchSize = 64;
    ui64 MaxMemoryBytes = 128 * 1024 * 1024;
    std::shared_ptr<TMemoryQuota> Quota;
    TClock Clock = [] { return std::chrono::steady_clock::now(); };
    TTraceHook Trace;
};
struct TSchedulerStats {
    size_t Queued = 0;
    size_t InFlight = 0; // held until physical cleanup, including after cancellation
    ui64 MemoryBytes = 0;
    ui64 Submitted = 0;
    ui64 Dispatched = 0;
    ui64 Dispatches = 0;
    ui64 Completed = 0;
    ui64 Cancelled = 0;
    ui64 Errors = 0;
    ui64 Timeouts = 0;
    ui64 QueueWaitUs = 0;
    ui64 ExecutionWaitUs = 0;
};

// Owner-driven bounded queue over D1. Submit never calls the backend. Flush at
// the end of available local work (also on CPU Yield) sends partial batches.
// No timer/polling to fill a batch. Operation deadlines use NextDeadline/Expire.
class TScheduler {
public:
    TScheduler(TWakeup wakeup, TLimits limits = {}, TSchedulerSettings settings = {});
    ~TScheduler();
    void RegisterProvider(TString name, std::shared_ptr<IProvider> provider);
    bool HasProvider(TStringBuf name) const;
    TSubmission Submit(TStringBuf provider, TRequest request);
    bool Cancel(TOperationId id);
    // Returns true only if dispatchable work remains after the budget ran out.
    bool Flush(const std::function<bool()>& tryStep);
    std::vector<TResult> TakeReady();
    std::optional<TTimePoint> NextDeadline() const;
    size_t ExpireDeadlines();
    void Shutdown();
    ui64 GetGeneration() const;
    TStats GetStats() const;
    TSchedulerStats GetSchedulerStats() const;
    std::shared_ptr<TMemoryQuota> GetQuota() const;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
} // namespace NKikimr::NMiniKQL::NAsync
