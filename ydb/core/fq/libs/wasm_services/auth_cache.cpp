#include "auth_cache.h"

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <mutex>

namespace NFq::NWasmServices {
namespace {
void CancelQuietly(TAuthCache::TCancel cancel) noexcept {
    try {
        if (cancel) {
            cancel();
        }
    } catch (...) {
    }
}

void CompleteQuietly(ICredentialProvider::TCompletion completion, TCredentialResult result) noexcept {
    try {
        if (completion) {
            completion(std::move(result));
        }
    } catch (...) {
    }
}

class TWaiter final: public ICredentialRequest {
public:
    explicit TWaiter(TAuthCache::TCancel cancel)
        : Cancel_(std::move(cancel))
    {
    }

    ~TWaiter() override {
        Cancel();
    }

    void Cancel() noexcept override {
        // Handles can be cancelled concurrently by transport shutdown/deadline.
        TAuthCache::TCancel cancel;
        {
            std::lock_guard lock(Mutex_);
            cancel = std::move(Cancel_);
        }
        CancelQuietly(std::move(cancel));
    }

private:
    std::mutex Mutex_;
    TAuthCache::TCancel Cancel_;
};
}   // namespace

struct TAuthCache::TState {
    struct TPending {
        TInstant Deadline;
        ICredentialProvider::TCompletion Completion;
        ui64 Reservation;
    };

    struct TEntry {
        ui64 Generation = 0;
        TInstant ExpiresAt;
        TInstant RefreshDeadline;
        TString Ticket;
        bool Loading = false;
        THashMap<ui64, TPending> Waiters;
        TCancel Cancel;
    };

    mutable std::mutex Mutex;
    TLoader Loader;
    TAuthCacheLimits Limits;
    THashMap<TString, TEntry> Entries;
    ui64 Generation = 0;
    ui64 NextWaiter = 0;
    ui64 Waiters = 0;
    ui64 Bytes = 0;
    bool Closed = false;

    void Remove(THashMap<TString, TEntry>::iterator it) {
        Bytes -= it->first.size() + MaxCredentialBytes + sizeof(TEntry);
        for (const auto& [_, waiter] : it->second.Waiters) {
            Bytes -= waiter.Reservation;
        }
        Waiters -= it->second.Waiters.size();
        Entries.erase(it);
    }
};

TAuthCache::TAuthCache(TLoader loader, TAuthCacheLimits limits)
    : State_(std::make_shared<TState>())
{
    Y_ENSURE(loader && limits.MaxEntries && limits.MaxWaiters && limits.MaxBytes >= MaxCredentialBytes + 4096 &&
                 limits.RefreshTimeout > TDuration::Zero(), "Invalid auth cache configuration");
    State_->Loader = std::move(loader);
    State_->Limits = limits;
}

TAuthCache::~TAuthCache() {
    Close();
}

std::shared_ptr<ICredentialRequest> TAuthCache::Request(const TString& key, TInstant deadline, ICredentialProvider::TCompletion completion) {
    auto state = State_;
    ui64 generation = 0, waiter = 0;
    bool load = false, immediate = false;
    TCredentialResult result;
    TInstant refreshDeadline;
    {
        std::lock_guard lock(state->Mutex);
        const auto now = TInstant::Now();
        const ui64 waiterBytes = key.size() + sizeof(TState::TPending) + 512;
        const ui64 entryBytes = key.size() + MaxCredentialBytes + sizeof(TState::TEntry);
        if (state->Closed || key.empty() || key.size() > 4096 || deadline <= now) {
            immediate = true;
        } else {
            auto it = state->Entries.find(key);
            if (it != state->Entries.end() && !it->second.Loading && it->second.ExpiresAt > now + state->Limits.ExpiryMargin) {
                result = { ECredentialStatus::Ready, it->second.Ticket };
                immediate = true;
            } else if (state->Waiters >= state->Limits.MaxWaiters || waiterBytes > state->Limits.MaxBytes - state->Bytes) {
                immediate = true;
            } else {
                if (it == state->Entries.end()) {
                    // Evict only idle entries; never strand waiters for admission.
                    while (
                        state->Entries.size() >= state->Limits.MaxEntries || state->Bytes + entryBytes + waiterBytes > state->Limits.MaxBytes) {
                        auto victim = state->Entries.end();
                        for (auto e = state->Entries.begin(); e != state->Entries.end(); ++e) {
                            if (!e->second.Loading && (victim == state->Entries.end() || e->second.ExpiresAt < victim->second.ExpiresAt)) {
                                victim = e;
                            }
                        }
                        if (victim == state->Entries.end()) {
                            break;
                        }
                        state->Remove(victim);
                    }
                    if (state->Entries.size() >= state->Limits.MaxEntries || state->Bytes + entryBytes + waiterBytes > state->Limits.MaxBytes) {
                        immediate = true;
                    } else {
                        it = state->Entries.emplace(key, TState::TEntry{}).first;
                        state->Bytes += entryBytes;
                    }
                }
                if (!immediate) {
                    auto& entry = it->second;
                    if (!entry.Loading) {
                        entry.Ticket.clear();
                        entry.Loading = true;
                        entry.Generation = ++state->Generation;
                        entry.RefreshDeadline = now + state->Limits.RefreshTimeout;
                        load = true;
                    }
                    generation = entry.Generation;
                    refreshDeadline = entry.RefreshDeadline;
                    waiter = ++state->NextWaiter;
                    entry.Waiters.emplace(waiter, TState::TPending{ deadline, std::move(completion), waiterBytes });
                    ++state->Waiters;
                    state->Bytes += waiterBytes;
                }
            }
        }
    }
    if (immediate) {
        CompleteQuietly(std::move(completion), std::move(result));
        return std::make_shared<TWaiter>(TCancel{});
    }
    const std::weak_ptr<TState> weak = state;
    auto request = std::make_shared<TWaiter>([weak, key, generation, waiter] {
        TCancel cancel;
        if (auto state = weak.lock()) {
            {
                std::lock_guard lock(state->Mutex);
                auto it = state->Entries.find(key);
                if (it == state->Entries.end() || it->second.Generation != generation) {
                    return;
                }
                if (auto w = it->second.Waiters.find(waiter); w != it->second.Waiters.end()) {
                    state->Bytes -= w->second.Reservation;
                    it->second.Waiters.erase(w);
                    --state->Waiters;
                }
                if (it->second.Loading && it->second.Waiters.empty()) {
                    cancel = std::move(it->second.Cancel);
                    state->Remove(it);
                }
            }
            CancelQuietly(std::move(cancel));
        }
    });
    if (load) {
        auto completed = [weak, key, generation](TAuthRefreshResult refresh) {
            TVector<TState::TPending> ready;
            TCancel cancel;
            if (auto state = weak.lock()) {
                {
                    std::lock_guard lock(state->Mutex);
                    auto it = state->Entries.find(key);
                    if (state->Closed || it == state->Entries.end() || !it->second.Loading || it->second.Generation != generation) {
                        return;
                    }
                    auto& entry = it->second;
                    const auto now = TInstant::Now();
                    bool valid = refresh.Credential.Status == ECredentialStatus::Ready && !refresh.Credential.Ticket.empty() &&
                                 refresh.Credential.Ticket.size() <= MaxCredentialBytes &&
                                 refresh.ExpiresAt > now + state->Limits.ExpiryMargin && now < entry.RefreshDeadline;
                    for (unsigned char c : refresh.Credential.Ticket) {
                        valid &= c >= 32 && c < 127;
                    }
                    if (!valid) {
                        refresh.Credential = {};
                    }
                    for (auto& [_, pending] : entry.Waiters) {
                        state->Bytes -= pending.Reservation;
                        ready.push_back(std::move(pending));
                    }
                    state->Waiters -= entry.Waiters.size();
                    entry.Waiters.clear();
                    cancel = std::move(entry.Cancel);
                    if (valid) {
                        entry.Loading = false;
                        entry.Ticket = refresh.Credential.Ticket;
                        entry.ExpiresAt = refresh.ExpiresAt;

                    } else {
                        state->Remove(it);
                    }
                }
                CancelQuietly(std::move(cancel));
                for (auto& pending : ready) {
                    CompleteQuietly(
                        std::move(pending.Completion), pending.Deadline > TInstant::Now() ? refresh.Credential : TCredentialResult{});
                }
            }
        };
        TCancel cancel;
        try {
            cancel = state->Loader(key, refreshDeadline, completed);
        } catch (...) {
            completed({});
        }
        bool keep;
        {
            std::lock_guard lock(state->Mutex);
            auto it = state->Entries.find(key);
            keep = !state->Closed && it != state->Entries.end() && it->second.Loading && it->second.Generation == generation;
            if (keep) {
                it->second.Cancel = std::move(cancel);
            }
        }
        if (!keep) {
            CancelQuietly(std::move(cancel));
        }
    }
    return request;
}

void TAuthCache::Expire(TInstant now) {
    TVector<ICredentialProvider::TCompletion> failed;
    TVector<TCancel> cancels;
    {
        std::lock_guard lock(State_->Mutex);
        for (auto it = State_->Entries.begin(); it != State_->Entries.end();) {
            auto& entry = it->second;
            for (auto w = entry.Waiters.begin(); w != entry.Waiters.end();) {
                if (w->second.Deadline <= now || entry.RefreshDeadline <= now) {
                    failed.push_back(std::move(w->second.Completion));
                    --State_->Waiters;
                    State_->Bytes -= w->second.Reservation;
                    entry.Waiters.erase(w++);
                } else {
                    ++w;
                }
            }
            if ((entry.Loading && entry.Waiters.empty()) || (!entry.Loading && entry.ExpiresAt <= now)) {
                cancels.push_back(std::move(entry.Cancel));
                auto victim = it++;
                State_->Remove(victim);
            } else {
                ++it;
            }
        }
    }
    for (auto& cancel : cancels) {
        CancelQuietly(std::move(cancel));
    }
    for (auto& completion : failed) {
        CompleteQuietly(std::move(completion), {});
    }
}

TInstant TAuthCache::NextDeadline() const {
    std::lock_guard lock(State_->Mutex);
    auto result = TInstant::Max();
    for (const auto& [_, entry] : State_->Entries) {
        if (entry.Loading) {
            result = std::min(result, entry.RefreshDeadline);
        }
        for (const auto& [_, waiter] : entry.Waiters) {
            result = std::min(result, waiter.Deadline);
        }
    }
    return result;
}

void TAuthCache::Close() {
    TVector<TCancel> cancels;
    TVector<ICredentialProvider::TCompletion> failed;
    {
        std::lock_guard lock(State_->Mutex);
        State_->Closed = true;
        for (auto& [_, entry] : State_->Entries) {
            cancels.push_back(std::move(entry.Cancel));
            for (auto& [_, waiter] : entry.Waiters) {
                failed.push_back(std::move(waiter.Completion));
            }
        }
        State_->Entries.clear();
        State_->Waiters = State_->Bytes = 0;
    }
    for (auto& cancel : cancels) {
        CancelQuietly(std::move(cancel));
    }
    for (auto& completion : failed) {
        CompleteQuietly(std::move(completion), {});
    }
}

TAuthCache::TStats TAuthCache::Stats() const {
    std::lock_guard lock(State_->Mutex);
    return { State_->Entries.size(), State_->Waiters, State_->Bytes };
}
}   // namespace NFq::NWasmServices
