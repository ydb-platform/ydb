#include "tvm.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_cookie.h>

#include <mutex>

namespace NFq::NWasmServices {
namespace {

class TMockTvmRequest final: public ITvmTicketRequest {
public:
    explicit TMockTvmRequest(ITvmTicketProvider::TCompletion completion)
        : Completion(std::move(completion))
    {
    }

    ~TMockTvmRequest() override {
        Cancel();
    }

    void Complete() {
        ITvmTicketProvider::TCompletion completion;
        {
            std::lock_guard lock(Mutex);
            completion = std::exchange(Completion, {});
            CancelTimer = {};
        }
        if (completion) {
            completion({ .Status = ETvmTicketStatus::Ready, .Ticket = "fqrun-mock-tvm-service-ticket" });
        }
    }

    void SetCancel(std::function<void()> cancel) {
        {
            std::lock_guard lock(Mutex);
            if (Completion) {
                CancelTimer = std::move(cancel);
                return;
            }
        }
        if (cancel) {
            cancel();   // completion/cancellation raced timer registration
        }
    }

    void Cancel() noexcept override {
        std::function<void()> cancel;
        {
            std::lock_guard lock(Mutex);
            Completion = {};
            cancel = std::exchange(CancelTimer, {});
        }
        if (cancel) {
            cancel();
        }
    }

private:
    std::mutex Mutex;
    ITvmTicketProvider::TCompletion Completion;
    std::function<void()> CancelTimer;
};

// A one-shot timer in the same user-pool mailbox. No polling and no worker thread.
class TMockTimer final: public NActors::TActorBootstrapped<TMockTimer> {
public:
    TMockTimer(TDuration delay, std::function<void()> complete)
        : Delay(delay)
        , Complete(std::move(complete))
    {
    }

    void Bootstrap() {
        Become(&TMockTimer::StateWork);
        Cookie.Reset(NActors::ISchedulerCookie::Make2Way());
        Schedule(Delay, new NActors::TEvents::TEvWakeup, Cookie.Get());
    }

private:
    STRICT_STFUNC(StateWork,
        cFunc(NActors::TEvents::TEvWakeup::EventType, OnTimer);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);)

    void OnTimer() {
        Complete();
        PassAway();
    }

    TDuration Delay;
    std::function<void()> Complete;
    NActors::TSchedulerCookieHolder Cookie;
};

std::function<void()> ScheduleMock(TDuration delay, std::function<void()> complete) {
    auto* system = NActors::TActivationContext::ActorSystem();
    const auto id = NActors::TActivationContext::AsActorContext().RegisterWithSameMailbox(new TMockTimer(delay, std::move(complete)));
    return [system, id] {
        system->Send(new NActors::IEventHandle(id, {}, new NActors::TEvents::TEvPoison));
    };
}

class TMockTvmProvider final: public ITvmTicketProvider {
public:
    TMockTvmProvider(TDuration delay, TMockTvmScheduler scheduler)
        : Delay(delay)
        , Scheduler(std::move(scheduler))
    {
    }

    std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials&, TInstant, TCompletion completion) override {
        auto request = std::make_shared<TMockTvmRequest>(std::move(completion));
        if (!Delay) {
            request->Complete();
        } else {
            request->SetCancel(Scheduler(Delay, [weak = std::weak_ptr<TMockTvmRequest>(request)] {
                if (auto pending = weak.lock()) {
                    pending->Complete();
                }
            }));
        }
        return request;
    }

private:
    TDuration Delay;
    TMockTvmScheduler Scheduler;
};

}   // namespace

std::shared_ptr<ITvmTicketProvider> CreateMockTvmTicketProvider(TDuration delay, TMockTvmScheduler scheduler) {
    return std::make_shared<TMockTvmProvider>(delay, scheduler ? std::move(scheduler) : ScheduleMock);
}

}   // namespace NFq::NWasmServices
