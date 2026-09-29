#include <ydb/library/yql/providers/native/operation_context.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_cookie.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>

#include <atomic>
#include <mutex>

namespace NYql::NNative {
namespace {

using namespace NActors;
using TLease = std::shared_ptr<void>;
using TPromise = NThreading::TPromise<TLease>;

struct TGovernorState {
    TActorSystem* System = nullptr;
    TActorId Actor;
    // At system shutdown, leases must keep the manager (and its RM state) alive
    // even after the governor actor has been destroyed.
    NDq::IMemoryQuotaManager::TPtr Quota;
    std::atomic<bool> Closed = false;
    std::atomic<ui64> NextRequest = 0;
    std::mutex Mutex;
};

struct TEvAcquire : TEventLocal<TEvAcquire, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    TEvAcquire(ui64 id, ui64 bytes, TInstant deadline, NThreading::TCancellationToken cancellation, TPromise promise)
        : Id(id), Bytes(bytes), Deadline(deadline), Cancellation(std::move(cancellation)), Promise(std::move(promise)) {}
    ui64 Id;
    ui64 Bytes;
    TInstant Deadline;
    NThreading::TCancellationToken Cancellation;
    TPromise Promise;
};
struct TEvRelease : TEventLocal<TEvRelease, EventSpaceBegin(TEvents::ES_PRIVATE) + 1> {
    explicit TEvRelease(ui64 bytes) : Bytes(bytes) {}
    ui64 Bytes;
};
struct TEvClose : TEventLocal<TEvClose, EventSpaceBegin(TEvents::ES_PRIVATE) + 2> {};
struct TEvCancel : TEventLocal<TEvCancel, EventSpaceBegin(TEvents::ES_PRIVATE) + 3> {
    explicit TEvCancel(ui64 id) : Id(id) {}
    ui64 Id;
};

void Reject(TPromise& promise, const char* message) {
    promise.SetException(std::make_exception_ptr(yexception() << message));
}

class TMemoryLease {
public:
    TMemoryLease(std::shared_ptr<TGovernorState> state, ui64 bytes)
        : State_(std::move(state)), Bytes_(bytes) {}
    ~TMemoryLease() {
        if (Active_) {
            std::lock_guard lock(State_->Mutex);
            if (State_->System) {
                State_->System->Send(State_->Actor, new TEvRelease(Bytes_));
            }
        }
    }
    void Activate() { Active_ = true; }
private:
    const std::shared_ptr<TGovernorState> State_;
    const ui64 Bytes_;
    bool Active_ = false;
};

class TMemoryGovernor final : public TActorBootstrapped<TMemoryGovernor> {
public:
    TMemoryGovernor(std::shared_ptr<TGovernorState> state, NDq::IMemoryQuotaManager::TPtr quota, ui64 limit)
        : State_(std::move(state)), Quota_(std::move(quota)), Limit_(limit) {}

    ~TMemoryGovernor() override {
        {
            std::lock_guard lock(State_->Mutex);
            State_->System = nullptr;
            State_->Closed.store(true);
        }
        for (auto& [_, request] : Pending_) {
            Reject(request->Get()->Promise, "Native memory admission stopped");
        }
    }

    void Bootstrap() {
        Become(&TMemoryGovernor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvAcquire, Handle);
        hFunc(TEvRelease, Handle);
        hFunc(TEvCancel, Handle);
        cFunc(TEvClose::EventType, Close);
        cFunc(TEvents::TEvWakeup::EventType, Retry);
    )

private:
    void Handle(TEvAcquire::TPtr& ev) {
        auto& request = *ev->Get();
        if (State_->Closed.load()) {
            Reject(request.Promise, "Native memory admission is closed");
        } else if (!request.Bytes || request.Bytes > Limit_ || !Quota_) {
            Reject(request.Promise, "Native memory request exceeds the operation limit");
        } else if (request.Deadline == TInstant::Max()) {
            Reject(request.Promise, "Native memory admission requires a finite deadline");
        } else {
            Pending_.emplace(request.Id, std::move(ev));
        }
        Admit();
    }

    void Handle(TEvCancel::TPtr& ev) {
        auto it = Pending_.find(ev->Get()->Id);
        if (it != Pending_.end()) {
            Reject(it->second->Get()->Promise, "Native memory admission cancelled");
            Pending_.erase(it);
        }
        Admit();
    }

    void Handle(TEvRelease::TPtr& ev) {
        const auto bytes = ev->Get()->Bytes;
        Y_ABORT_UNLESS(ActiveBytes_ >= bytes && ActiveLeases_);
        Quota_->FreeQuota(bytes);
        ActiveBytes_ -= bytes;
        --ActiveLeases_;
        Admit();
    }

    void Close() {
        Closed_ = true;
        State_->Closed.store(true);
        Timer_.Detach();
        for (auto& [_, request] : Pending_) {
            Reject(request->Get()->Promise, "Native memory admission is closed");
        }
        Pending_.clear();
        MaybeStop();
    }

    void Retry() {
        Timer_.Detach();
        Admit();
    }

    void Admit() {
        if (Closed_) {
            MaybeStop();
            return;
        }
        if (State_->Closed.load()) {
            // Shutdown's close event follows all accepted acquire events. Do not
            // stop early and abandon a promise still queued in this mailbox.
            Timer_.Detach();
            return;
        }
        const auto now = TActivationContext::Now();
        TInstant next = now + TDuration::MilliSeconds(25);
        for (auto it = Pending_.begin(); it != Pending_.end();) {
            auto& request = *it->second->Get();
            bool done = true;
            if (request.Cancellation.IsCancellationRequested()) {
                Reject(request.Promise, "Native memory admission cancelled");
            } else if (request.Deadline <= now) {
                Reject(request.Promise, "Native memory admission deadline exceeded");
            } else {
                // Allocate the small ownership record first: failure must not
                // strand a successfully acquired quota reservation.
                auto lease = std::make_shared<TMemoryLease>(State_, request.Bytes);
                if (request.Bytes <= Limit_ - ActiveBytes_ && Quota_->AllocateQuota(request.Bytes, true)) {
                    lease->Activate();
                    ActiveBytes_ += request.Bytes;
                    ++ActiveLeases_;
                    request.Promise.SetValue(std::move(lease));
                } else {
                    next = Min(next, request.Deadline);
                    done = false;
                }
            }
            if (done) {
                Pending_.erase(it++);
            } else {
                ++it;
            }
        }
        if (Pending_.empty()) {
            Timer_.Detach();
        } else if (!Timer_.Get()) {
            Timer_.Reset(ISchedulerCookie::Make2Way());
            Schedule(next, new TEvents::TEvWakeup(), Timer_.Get());
        }
    }

    void MaybeStop() {
        if (Closed_ && !ActiveLeases_) {
            Timer_.Detach();
            TActorBootstrapped::PassAway();
        }
    }

    const std::shared_ptr<TGovernorState> State_;
    const NDq::IMemoryQuotaManager::TPtr Quota_;
    const ui64 Limit_;
    ui64 ActiveBytes_ = 0;
    ui64 ActiveLeases_ = 0;
    bool Closed_ = false;
    THashMap<ui64, TEvAcquire::TPtr> Pending_;
    TSchedulerCookieHolder Timer_;
};

class TAsyncMemoryQuota final : public IAsyncMemoryQuota {
public:
    explicit TAsyncMemoryQuota(std::shared_ptr<TGovernorState> state) : State_(std::move(state)) {}
    ~TAsyncMemoryQuota() override { Shutdown(); }

    NThreading::TFuture<TLease> Acquire(ui64 bytes, TInstant deadline, NThreading::TCancellationToken cancellation) override {
        auto promise = NThreading::NewPromise<TLease>();
        auto result = promise.GetFuture();
        const auto id = ++State_->NextRequest;
        {
            std::lock_guard lock(State_->Mutex);
            if (State_->Closed.load()) {
                Reject(promise, "Native memory admission is closed");
                return result;
            }
            State_->System->Send(State_->Actor, new TEvAcquire(id, bytes, deadline, cancellation, promise));
        }
        if (cancellation.Future().StateId() != NThreading::TCancellationToken::Default().Future().StateId()) {
            cancellation.Future().Subscribe([weak = std::weak_ptr<TGovernorState>(State_), id](const auto&) {
                if (auto state = weak.lock()) {
                    std::lock_guard lock(state->Mutex);
                    if (!state->Closed.load()) {
                        state->System->Send(state->Actor, new TEvCancel(id));
                    }
                }
            });
        }
        return result;
    }

    void Shutdown() override {
        std::lock_guard lock(State_->Mutex);
        if (!State_->Closed.exchange(true)) {
            State_->System->Send(State_->Actor, new TEvClose());
        }
    }
private:
    const std::shared_ptr<TGovernorState> State_;
};

} // namespace

std::shared_ptr<IAsyncMemoryQuota> CreateAsyncMemoryQuota(
    NActors::TActorSystem* system, NDq::IMemoryQuotaManager::TPtr quota, ui64 limit,
    std::function<NActors::TActorId(NActors::IActor*)> registerActor) {
    Y_ENSURE(system, "Native memory admission requires an actor system");
    auto state = std::make_shared<TGovernorState>();
    state->System = system;
    state->Quota = quota;
    auto* actor = new TMemoryGovernor(state, std::move(quota), limit);
    state->Actor = registerActor ? registerActor(actor) : system->Register(actor);
    return std::make_shared<TAsyncMemoryQuota>(std::move(state));
}

} // namespace NYql::NNative
