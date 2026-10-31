#include "async.h"

#include <util/generic/yexception.h>
#include <util/system/yassert.h>

#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <utility>

namespace NKikimr::NMiniKQL::NAsync {
namespace {

ui64 NewGeneration() {
    static std::atomic<ui64> next{1};
    const auto value = next.fetch_add(1, std::memory_order_relaxed);
    Y_ABORT_UNLESS(value != 0, "Async generation exhausted");
    return value;
}

struct TEntry {
    TString Provider;
    std::shared_ptr<const TRequest> Request;
    std::optional<TTimePoint> Deadline;
    ui64 MaxResponseBytes = 0;
    ui64 Reservation = 0;
    std::optional<TResult> Result;
    bool Finished = false;
    bool Taken = false;
    bool Released = false;
};

// Completion tokens own this state, which deliberately does not own providers.
// This avoids provider -> completion -> registry -> provider reference cycles.
struct TState {
    const ui64 Generation = NewGeneration();
    const TLimits Limits;
    const TClock Clock;
    mutable std::mutex Mutex;
    std::map<ui64, TEntry> Entries;
    std::deque<ui64> Ready;
    TWakeup Wakeup;
    ui64 Sequence = 0;
    ui64 Reserved = 0;
    bool Notified = false;
    bool Closed = false;

    TState(TWakeup wakeup, TLimits limits, TClock clock)
        : Limits(limits)
        , Clock(std::move(clock))
        , Wakeup(std::move(wakeup))
    {
    }

    // Mutex held. Readiness includes newly available admission credits.
    TWakeup Notify() {
        if (!Closed && !Notified) {
            Notified = true;
            return Wakeup;
        }
        return {};
    }

    // Mutex held. All callbacks, including wakeup, run after releasing it.
    TWakeup Finish(ui64 sequence, TEntry& entry, EOutcome outcome, EError error,
                   TString payload, TTimePoint now) {
        if (entry.Finished) {
            return {};
        }
        if (entry.Deadline && now >= *entry.Deadline) {
            outcome = EOutcome::Timeout;
            error = EError::None;
            payload.clear();
        } else if (payload.size() > entry.MaxResponseBytes) {
            outcome = EOutcome::Error;
            error = EError::ResponseTooLarge;
            payload.clear();
        }
        entry.Finished = true;
        entry.Result = TResult{{Generation, sequence}, outcome, error, std::move(payload), entry.Request->Reservation};
        Ready.push_back(sequence);
        return Notify();
    }

    void EraseIfDone(std::map<ui64, TEntry>::iterator it) {
        if (it->second.Taken && it->second.Released) {
            Reserved -= it->second.Reservation;
            Entries.erase(it);
        }
    }

    bool Publish(TOperationId id, EOutcome outcome, EError error, TString payload) noexcept {
        const auto now = Clock();
        TWakeup notify;
        {
            std::lock_guard guard(Mutex);
            const auto it = Entries.find(id.Sequence);
            if (id.Generation != Generation || it == Entries.end() || it->second.Finished) {
                return false;
            }
            notify = Finish(id.Sequence, it->second, outcome, error, std::move(payload), now);
        }
        if (notify) {
            notify(Generation);
        }
        return true;
    }

    void Release(TOperationId id) noexcept {
        const auto now = Clock();
        TWakeup notify;
        std::shared_ptr<const TRequest> releasedRequest;
        {
            std::lock_guard guard(Mutex);
            const auto it = Entries.find(id.Sequence);
            if (id.Generation != Generation || it == Entries.end() || it->second.Released) {
                return;
            }
            auto& entry = it->second;
            notify = Finish(id.Sequence, entry, EOutcome::Error, EError::Abandoned, {}, now);
            entry.Released = true;
            releasedRequest = std::move(entry.Request);
            const bool returnsCredits = entry.Taken;
            EraseIfDone(it);
            if (returnsCredits) {
                notify = Notify();
            }
        }
        if (notify) {
            notify(Generation);
        }
    }
};

class TCompletion final: public ICompletion {
public:
    TCompletion(std::shared_ptr<TState> state, TOperationId id)
        : State(std::move(state))
        , Id(id)
    {
    }

    ~TCompletion() override {
        Release();
    }

    TOperationId GetId() const noexcept override {
        return Id;
    }

    void Complete(TString payload) noexcept override {
        State->Publish(Id, EOutcome::Success, EError::None, std::move(payload));
    }

    void Fail(TString payload) noexcept override {
        State->Publish(Id, EOutcome::Error, EError::Provider, std::move(payload));
    }

    void Release() noexcept override {
        State->Release(Id);
    }

private:
    const std::shared_ptr<TState> State;
    const TOperationId Id;
};

} // namespace

struct TExecutionContext::TImpl {
    std::map<TString, std::shared_ptr<IProvider>> Providers;
    const std::shared_ptr<TState> State;

    TImpl(TWakeup wakeup, TLimits limits, TClock clock)
        : State(std::make_shared<TState>(std::move(wakeup), limits, std::move(clock)))
    {
    }
};

TExecutionContext::TExecutionContext(TWakeup wakeup, TLimits limits, TClock clock) {
    Y_ENSURE(wakeup, "Async context requires a wakeup callback");
    Y_ENSURE(clock, "Async context requires a clock");
    Impl = std::make_unique<TImpl>(std::move(wakeup), limits, std::move(clock));
}

TExecutionContext::~TExecutionContext() {
    Shutdown();
    // Discard owner results, but tokens keep physical reservations/input alive
    // until provider cleanup. They contain no pointer back to this context.
    auto& state = *Impl->State;
    std::vector<TResult> discarded;
    std::lock_guard guard(state.Mutex);
    state.Ready.clear();
    for (auto it = state.Entries.begin(); it != state.Entries.end();) {
        auto current = it++;
        current->second.Taken = true;
        if (current->second.Result) {
            discarded.push_back(std::move(*current->second.Result));
        }
        current->second.Result.reset();
        state.EraseIfDone(current);
    }
}

void TExecutionContext::RegisterProvider(TString name, std::shared_ptr<IProvider> provider) {
    Y_ENSURE(!name.empty() && provider, "Async provider requires a name and implementation");
    Y_ENSURE(!GetStats().Closed, "Async context is closed");
    Y_ENSURE(Impl->Providers.emplace(std::move(name), std::move(provider)).second,
             "Async provider is already registered");
}

TSubmission TExecutionContext::Submit(TStringBuf providerName, TRequest request) {
    auto state = Impl->State;
    const auto provider = Impl->Providers.find(TString(providerName));
    const auto now = state->Clock();
    std::shared_ptr<const TRequest> owned;
    TOperationId id;
    {
        std::lock_guard guard(state->Mutex);
        if (state->Closed) {
            return {EAdmission::Closed, {}};
        }
        if (provider == Impl->Providers.end()) {
            return {EAdmission::UnknownProvider, {}};
        }
        if (request.Deadline && now >= *request.Deadline) {
            return {EAdmission::DeadlineExceeded, {}};
        }
        if (state->Entries.size() >= state->Limits.MaxOperations) {
            return {EAdmission::OperationsLimit, {}};
        }
        ui64 bytes = request.Operation.size();
        if (request.Payload.size() > state->Limits.MaxPayloadBytes || bytes > state->Limits.MaxPayloadBytes - request.Payload.size()) {
            return {EAdmission::BytesLimit, {}};
        }
        bytes += request.Payload.size();
        if (request.MaxResponseBytes > state->Limits.MaxPayloadBytes - bytes) {
            return {EAdmission::BytesLimit, {}};
        }
        bytes += request.MaxResponseBytes;
        if (bytes > state->Limits.MaxPayloadBytes - state->Reserved) {
            return {EAdmission::BytesLimit, {}};
        }
        Y_ENSURE(state->Sequence != std::numeric_limits<ui64>::max(), "Async sequence exhausted");
        id = {state->Generation, ++state->Sequence};
        owned = std::make_shared<TRequest>(std::move(request));
        TEntry entry;
        entry.Provider = TString(providerName);
        entry.Request = owned;
        entry.Deadline = owned->Deadline;
        entry.MaxResponseBytes = owned->MaxResponseBytes;
        entry.Reservation = bytes;
        state->Entries.emplace(id.Sequence, std::move(entry));
        state->Reserved += bytes;
    }
    // Register before Start: synchronous completion is indistinguishable from
    // delayed completion. Keeping a local token also covers a throwing Start.
    auto completion = std::make_shared<TCompletion>(state, id);
    try {
        provider->second->Start(id, std::move(owned), completion);
    } catch (...) {
        if (state->Publish(id, EOutcome::Error, EError::StartFailed, {})) {
            provider->second->Cancel(id);
        }
    }
    return {EAdmission::Accepted, id};
}

bool TExecutionContext::Cancel(TOperationId id) {
    auto state = Impl->State;
    TString provider;
    TWakeup notify;
    const auto now = state->Clock();
    {
        std::lock_guard guard(state->Mutex);
        const auto it = state->Entries.find(id.Sequence);
        if (id.Generation != state->Generation || it == state->Entries.end() || it->second.Finished) {
            return false;
        }
        provider = it->second.Provider;
        notify = state->Finish(id.Sequence, it->second, EOutcome::Cancelled, EError::None, {}, now);
    }
    Impl->Providers.at(provider)->Cancel(id);
    if (notify) {
        notify(state->Generation);
    }
    return true;
}

std::optional<TTimePoint> TExecutionContext::NextDeadline() const {
    auto& state = *Impl->State;
    std::lock_guard guard(state.Mutex);
    std::optional<TTimePoint> next;
    for (const auto& [sequence, entry] : state.Entries) {
        if (!entry.Finished && entry.Deadline && (!next || *entry.Deadline < *next)) {
            next = entry.Deadline;
        }
    }
    return next;
}

size_t TExecutionContext::ExpireDeadlines() {
    const auto now = Impl->State->Clock();
    std::vector<TOperationId> expired;
    {
        std::lock_guard guard(Impl->State->Mutex);
        for (const auto& [sequence, entry] : Impl->State->Entries) {
            if (!entry.Finished && entry.Deadline && now >= *entry.Deadline) {
                expired.push_back({GetGeneration(), sequence});
            }
        }
    }
    size_t count = 0;
    for (auto id : expired) {
        count += Cancel(id);
    }
    return count;
}

std::vector<TResult> TExecutionContext::TakeReady() {
    auto& state = *Impl->State;
    std::lock_guard guard(state.Mutex);
    std::vector<TResult> results;
    results.reserve(state.Ready.size());
    // Rearm before releasing the lock, not after invoking consumer code.
    state.Notified = false;
    while (!state.Ready.empty()) {
        const auto sequence = state.Ready.front();
        state.Ready.pop_front();
        auto it = state.Entries.find(sequence);
        auto& entry = it->second;
        results.push_back(std::move(*entry.Result));
        entry.Result.reset();
        entry.Taken = true;
        state.EraseIfDone(it);
    }
    return results;
}

void TExecutionContext::Shutdown() noexcept {
    auto state = Impl->State;
    const auto now = state->Clock();
    std::vector<std::pair<TOperationId, TString>> cancellations;
    // A capture may own the last completion token. Destroy it only after unlock
    // and cancellation dispatch: its destructor can reenter TState::Release.
    TWakeup detachedWakeup;
    {
        std::lock_guard guard(state->Mutex);
        if (state->Closed) {
            return;
        }
        state->Closed = true;
        detachedWakeup.swap(state->Wakeup);
        for (auto& [sequence, entry] : state->Entries) {
            if (!entry.Finished) {
                state->Finish(sequence, entry, EOutcome::Cancelled, EError::None, {}, now);
                cancellations.push_back({{state->Generation, sequence}, entry.Provider});
            }
        }
    }
    for (const auto& [id, provider] : cancellations) {
        Impl->Providers.at(provider)->Cancel(id);
    }
}

ui64 TExecutionContext::GetGeneration() const noexcept {
    return Impl->State->Generation;
}

TStats TExecutionContext::GetStats() const {
    auto& state = *Impl->State;
    std::lock_guard guard(state.Mutex);
    TStats stats;
    stats.TrackedOperations = state.Entries.size();
    stats.ReadyResults = state.Ready.size();
    stats.ReservedPayloadBytes = state.Reserved;
    stats.Closed = state.Closed;
    for (const auto& [sequence, entry] : state.Entries) {
        stats.PendingOperations += !entry.Finished;
        stats.CleanupPending += entry.Finished && !entry.Released;
    }
    return stats;
}

} // namespace NKikimr::NMiniKQL::NAsync
