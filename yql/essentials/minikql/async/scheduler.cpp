#include "scheduler.h"
#include <util/generic/yexception.h>
#include <algorithm>
#include <atomic>
#include <deque>
#include <map>
#include <mutex>
#include <set>

namespace NKikimr::NMiniKQL::NAsync {
namespace {
constexpr ui64 MaxKeyBytes = 1024;
// Conservative per-entry allowance for queue, lifecycle, completion and timing
// metadata. Payload and response bounds are charged separately, only once.
constexpr ui64 EntryBytes = 2048;
struct TShared {
    std::atomic<size_t> Active = 0;
    std::mutex Mutex;
    std::set<ui64> ActiveIds;
    std::atomic<bool> Closed = false;
    const TWakeup Wakeup;
    explicit TShared(TWakeup wakeup)
        : Wakeup(std::move(wakeup))
    {
    }
};
class TCleanup final: public ICompletion {
public:
    TCleanup(std::shared_ptr<TShared> state, std::shared_ptr<ICompletion> completion)
        : State_(std::move(state))
        , Completion_(std::move(completion))
    {
        std::lock_guard lock(State_->Mutex);
        State_->ActiveIds.insert(GetId().Sequence);
        ++State_->Active;
    }
    ~TCleanup() override {
        Release();
    }
    TOperationId GetId() const noexcept override {
        return Completion_->GetId();
    }
    void Complete(TString payload) noexcept override {
        Completion_->Complete(std::move(payload));
    }
    void Fail(TString payload) noexcept override {
        Completion_->Fail(std::move(payload));
    }
    void Release() noexcept override {
        if (!Released_.exchange(true)) {
            Completion_->Release();
            {
                std::lock_guard lock(State_->Mutex);
                State_->ActiveIds.erase(GetId().Sequence);
                --State_->Active;
            }
            if (!State_->Closed.load()) {
                State_->Wakeup(GetId().Generation);
            }
        }
    }

private:
    const std::shared_ptr<TShared> State_;
    const std::shared_ptr<ICompletion> Completion_;
    std::atomic<bool> Released_ = false;
};
struct TQueued {
    TBatchItem Item;
    TString Key;
    TTimePoint Enqueued;
};
class TQueuedProvider final: public IProvider {
public:
    const std::shared_ptr<IProvider> Backend;
    IBatchProvider* const Batch;
    const std::shared_ptr<TShared> Shared;
    const TClock Clock;
    std::deque<TQueued> Queue;
    TQueuedProvider(std::shared_ptr<IProvider> backend, std::shared_ptr<TShared> shared, TClock clock)
        : Backend(std::move(backend))
        , Batch(dynamic_cast<IBatchProvider*>(Backend.get()))
        , Shared(std::move(shared))
        , Clock(std::move(clock))
    {
    }
    void Start(TOperationId id, std::shared_ptr<const TRequest> request, std::shared_ptr<ICompletion> completion) override {
        TString key = Batch ? Batch->GetBatchKey(*request) : TString();
        Y_ENSURE(key.size() <= MaxKeyBytes, "Async batch compatibility key exceeds limit");
        Queue.push_back({{id, std::move(request), std::move(completion)}, std::move(key), Clock()});
    }
    void Cancel(TOperationId id) noexcept override {
        auto it = std::find_if(Queue.begin(), Queue.end(), [&](const auto& entry) { return entry.Item.Id == id; });
        if (it != Queue.end()) {
            auto completion = std::move(it->Item.Completion);
            Queue.erase(it);
            completion->Release();
        } else {
            bool active;
            {
                std::lock_guard lock(Shared->Mutex);
                active = Shared->ActiveIds.contains(id.Sequence);
            }
            if (active) {
                Backend->Cancel(id);
            }
        }
    }
};
ui64 Micros(TTimePoint start, TTimePoint end) {
    return end > start ? std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() : 0;
}
} // namespace
struct TScheduler::TImpl {
    TSchedulerSettings Settings;
    std::shared_ptr<TShared> Shared;
    std::shared_ptr<TMemoryQuota> Quota;
    TExecutionContext Context;
    std::map<TString, std::shared_ptr<TQueuedProvider>> ByName;
    std::vector<std::shared_ptr<TQueuedProvider>> Providers;
    std::map<ui64, TTimePoint> Started;
    TSchedulerStats Counters;
    size_t Cursor = 0;
    bool Closed = false;
    TImpl(TWakeup wakeup, TLimits limits, TSchedulerSettings settings)
        : Settings(std::move(settings))
        , Shared(std::make_shared<TShared>(std::move(wakeup)))
        , Quota(Settings.Quota ? Settings.Quota : std::make_shared<TMemoryQuota>(Settings.MaxMemoryBytes))
        , Context([s = Shared](ui64 generation) {
            if (!s->Closed.load()) {
                s->Wakeup(generation);
            }
        }, limits, Settings.Clock)
    {
        Y_ENSURE(Settings.MaxInFlight && Settings.MaxBatchSize && Settings.Clock, "Invalid async scheduler limits");
    }
    void Event(TExecutionEvent event) {
        if (Settings.Trace) {
            Settings.Trace(event);
        }
    }
};
TScheduler::TScheduler(TWakeup wakeup, TLimits limits, TSchedulerSettings settings)
    : Impl_(std::make_unique<TImpl>(std::move(wakeup), limits, std::move(settings)))
{
}
TScheduler::~TScheduler() {
    Shutdown();
}
void TScheduler::RegisterProvider(TString name, std::shared_ptr<IProvider> provider) {
    Y_ENSURE(provider && !Impl_->Closed && !Impl_->ByName.contains(name), "Invalid async provider registration");
    auto queued = std::make_shared<TQueuedProvider>(std::move(provider), Impl_->Shared, Impl_->Settings.Clock);
    Impl_->Context.RegisterProvider(name, queued);
    Impl_->ByName.emplace(std::move(name), queued);
    Impl_->Providers.push_back(std::move(queued));
}
bool TScheduler::HasProvider(TStringBuf name) const {
    return Impl_->ByName.contains(TString(name));
}
TSubmission TScheduler::Submit(TStringBuf provider, TRequest request) {
    auto& impl = *Impl_;
    if (impl.Closed) {
        return {EAdmission::Closed, {}};
    }
    if (!HasProvider(provider)) {
        return {EAdmission::UnknownProvider, {}};
    }
    // The scheduler owns this boundary. Reusing a caller's lease could disguise
    // different buffers or double-charge nested calls; providers borrow the lease
    // on the accepted request for these same buffers only.
    Y_ENSURE(!request.Reservation, "Async submission must not import a reservation");
    const ui64 limit = impl.Quota->GetLimit();
    ui64 bytes = EntryBytes + MaxKeyBytes;
    for (ui64 amount : {ui64(request.Operation.size()), ui64(request.Payload.size()), request.MaxResponseBytes}) {
        if (bytes > limit || amount > limit - bytes) {
            return {EAdmission::RequestTooLarge, {}};
        }
        bytes += amount;
    }
    const auto generation = GetGeneration();
    request.Reservation = impl.Quota->TryReserve(bytes, generation, [s = impl.Shared, generation] {
        if (!s->Closed.load()) {
            s->Wakeup(generation);
        }
    });
    if (!request.Reservation) {
        return {EAdmission::ResourceLimit, {}};
    }
    auto submitted = impl.Context.Submit(provider, std::move(request));
    if (submitted.Status == EAdmission::Accepted) {
        ++impl.Counters.Submitted;
        impl.Event({EEventKind::Submitted, submitted.Id});
    }
    return submitted;
}
bool TScheduler::Cancel(TOperationId id) {
    return Impl_->Context.Cancel(id);
}
bool TScheduler::Flush(const std::function<bool()>& tryStep) {
    auto& impl = *Impl_;
    if (impl.Closed || impl.Providers.empty()) {
        return false;
    }
    impl.Context.ExpireDeadlines();
    while (impl.Shared->Active.load() < impl.Settings.MaxInFlight) {
        std::shared_ptr<TQueuedProvider> provider;
        size_t index = impl.Cursor;
        for (size_t i = 0; i < impl.Providers.size(); ++i) {
            index = (impl.Cursor + i) % impl.Providers.size();
            if (!impl.Providers[index]->Queue.empty()) {
                provider = impl.Providers[index];
                break;
            }
        }
        if (!provider) {
            return false;
        }
        if (!tryStep()) {
            return true;
        }
        impl.Cursor = (index + 1) % impl.Providers.size();
        const auto& first = provider->Queue.front();
        const TString key = first.Key;
        const TString operation = first.Item.Request->Operation;
        const auto deadline = first.Item.Request->Deadline;
        const size_t maxBatch = provider->Batch ? std::max<size_t>(1, provider->Batch->GetMaxBatchSize()) : 1;
        const size_t count = std::min({maxBatch, impl.Settings.MaxBatchSize, impl.Settings.MaxInFlight - impl.Shared->Active.load()});
        std::vector<TBatchItem> items;
        const auto now = impl.Settings.Clock();
        if (deadline && now >= *deadline) {
            impl.Context.ExpireDeadlines();
            continue;
        }
        for (auto it = provider->Queue.begin(); it != provider->Queue.end() && items.size() < count;) {
            if (it->Key != key || it->Item.Request->Operation != operation || it->Item.Request->Deadline != deadline) {
                ++it;
                continue;
            }
            const auto wait = Micros(it->Enqueued, now);
            impl.Counters.QueueWaitUs += wait;
            impl.Started.emplace(it->Item.Id.Sequence, now);
            it->Item.Completion = std::make_shared<TCleanup>(impl.Shared, std::move(it->Item.Completion));
            impl.Event({EEventKind::Dispatched, it->Item.Id, EOutcome::Success, wait});
            items.push_back(std::move(it->Item));
            it = provider->Queue.erase(it);
        }
        ++impl.Counters.Dispatches;
        impl.Counters.Dispatched += items.size();
        try {
            if (provider->Batch) {
                provider->Batch->StartBatch(items);
            } else {
                const auto& item = items.front();
                provider->Backend->Start(item.Id, item.Request, item.Completion);
            }
        } catch (...) {
            for (auto& item : items) {
                item.Completion->Fail({});
                provider->Backend->Cancel(item.Id);
            }
        }
    }
    return false; // capacity wait, no runnable self-notification
}
std::vector<TResult> TScheduler::TakeReady() {
    auto& impl = *Impl_;
    auto results = impl.Context.TakeReady();
    for (const auto& result : results) {
        ++impl.Counters.Completed;
        impl.Counters.Cancelled += result.Outcome == EOutcome::Cancelled;
        impl.Counters.Errors += result.Outcome == EOutcome::Error;
        impl.Counters.Timeouts += result.Outcome == EOutcome::Timeout;
        ui64 wait = 0;
        if (auto it = impl.Started.find(result.Id.Sequence); it != impl.Started.end()) {
            wait = Micros(it->second, impl.Settings.Clock());
            impl.Started.erase(it);
        }
        impl.Counters.ExecutionWaitUs += wait;
        impl.Event({EEventKind::Completed, result.Id, result.Outcome, 0, wait});
    }
    return results;
}
std::optional<TTimePoint> TScheduler::NextDeadline() const {
    return Impl_->Context.NextDeadline();
}
size_t TScheduler::ExpireDeadlines() {
    return Impl_->Context.ExpireDeadlines();
}
void TScheduler::Shutdown() {
    if (Impl_->Closed) {
        return;
    }
    Impl_->Closed = true;
    Impl_->Shared->Closed.store(true);
    Impl_->Quota->RemoveWaiter(GetGeneration());
    Impl_->Context.Shutdown();
}
ui64 TScheduler::GetGeneration() const {
    return Impl_->Context.GetGeneration();
}
TStats TScheduler::GetStats() const {
    return Impl_->Context.GetStats();
}
std::shared_ptr<TMemoryQuota> TScheduler::GetQuota() const {
    return Impl_->Quota;
}
TSchedulerStats TScheduler::GetSchedulerStats() const {
    auto stats = Impl_->Counters;
    for (auto& provider : Impl_->Providers) {
        stats.Queued += provider->Queue.size();
    }
    stats.InFlight = Impl_->Shared->Active.load();
    stats.MemoryBytes = Impl_->Quota->GetUsed();
    return stats;
}
} // namespace NKikimr::NMiniKQL::NAsync
