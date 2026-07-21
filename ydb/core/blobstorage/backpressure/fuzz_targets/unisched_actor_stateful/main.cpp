#include <ydb/core/blobstorage/backpressure/unisched.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace {

using namespace NActors;
using namespace NKikimr;

constexpr size_t MaxSubscribers = 16;
constexpr size_t MaxOperations = 96;

class TSubscriberActor final : public TActorBootstrapped<TSubscriberActor> {
public:
    TSubscriberActor(TVector<ui64>& wakeups, ui64 index)
        : Wakeups(wakeups)
        , Index(index)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            cFunc(TEvents::TSystem::Poison, PassAway);
        }
    }

    void HandleWakeup() {
        Wakeups.push_back(Index);
    }

private:
    TVector<ui64>& Wakeups;
    const ui64 Index;
};

struct TSubscriber {
    TActorId ActorId;
    TIntrusivePtr<NBackpressure::TFlowRecord> FlowRecord;
    bool Registered = false;
    bool Alive = true;
};

void CheckSchedulerInvariant(const TVector<TSubscriber>& subscribers);

TTestActorRuntime::TEgg MakeEgg() {
    return {new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {}};
}

void Dispatch(TTestActorRuntime& runtime) {
    TDispatchOptions options;
    options.Quiet = true;
    options.FinalEvents.emplace_back([](IEventHandle&) { return true; }, 1);
    try {
        runtime.DispatchEvents(options, TDuration::MilliSeconds(1));
    } catch (const TEmptyEventQueueException&) {
    }
}

void RunUnischedActorFuzz(FuzzedDataProvider& fdp) {
    TTestActorRuntime runtime(1, false);
    runtime.Initialize(MakeEgg());

    const TActorId edge = runtime.AllocateEdgeActor();
    const TActorId scheduler = runtime.Register(CreateUniversalSchedulerActor());
    runtime.RegisterService(MakeUniversalSchedulerActorId(), scheduler);

    TVector<TSubscriber> subscribers;
    TVector<ui64> wakeups;

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 6)) {
            case 0: {
                if (subscribers.size() >= MaxSubscribers) {
                    break;
                }
                const ui64 index = subscribers.size();
                TSubscriber sub;
                sub.ActorId = runtime.Register(new TSubscriberActor(wakeups, index));
                sub.FlowRecord = MakeIntrusive<NBackpressure::TFlowRecord>();
                sub.FlowRecord->SetPredictedDelayNs(fdp.ConsumeIntegralInRange<TAtomicBase>(0, 1'000'000));
                subscribers.push_back(std::move(sub));
                Dispatch(runtime);
                break;
            }

            case 1: {
                TVector<size_t> unregistered;
                for (size_t i = 0; i < subscribers.size(); ++i) {
                    if (subscribers[i].Alive && !subscribers[i].Registered) {
                        unregistered.push_back(i);
                    }
                }
                if (!unregistered.empty()) {
                    auto& sub = subscribers[unregistered[fdp.ConsumeIntegralInRange<size_t>(0, unregistered.size() - 1)]];
                    Y_ABORT_UNLESS(RegisterActorInUniversalScheduler(sub.ActorId, sub.FlowRecord, runtime.GetActorSystem(0)));
                    sub.Registered = true;
                    Dispatch(runtime);
                }
                break;
            }

            case 2: {
                TVector<size_t> registered;
                for (size_t i = 0; i < subscribers.size(); ++i) {
                    if (subscribers[i].Alive && subscribers[i].Registered) {
                        registered.push_back(i);
                    }
                }
                if (!registered.empty()) {
                    auto& sub = subscribers[registered[fdp.ConsumeIntegralInRange<size_t>(0, registered.size() - 1)]];
                    Y_ABORT_UNLESS(RegisterActorInUniversalScheduler(sub.ActorId, nullptr, runtime.GetActorSystem(0)));
                    sub.Registered = false;
                    Dispatch(runtime);
                }
                break;
            }

            case 3: {
                if (!subscribers.empty()) {
                    auto& sub = subscribers[fdp.ConsumeIntegralInRange<size_t>(0, subscribers.size() - 1)];
                    sub.FlowRecord->SetPredictedDelayNs(fdp.ConsumeIntegralInRange<TAtomicBase>(0, 5'000'000));
                }
                break;
            }

            case 4:
                runtime.AdvanceCurrentTime(TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui64>(0, 2500)));
                if (std::any_of(subscribers.begin(), subscribers.end(), [](const TSubscriber& sub) { return sub.Registered; })) {
                    Dispatch(runtime);
                }
                break;

            case 5: {
                TVector<size_t> alive;
                for (size_t i = 0; i < subscribers.size(); ++i) {
                    if (subscribers[i].Alive) {
                        alive.push_back(i);
                    }
                }
                if (!alive.empty()) {
                    auto& sub = subscribers[alive[fdp.ConsumeIntegralInRange<size_t>(0, alive.size() - 1)]];
                    if (sub.Registered) {
                        Y_ABORT_UNLESS(RegisterActorInUniversalScheduler(sub.ActorId, nullptr, runtime.GetActorSystem(0)));
                        sub.Registered = false;
                        Dispatch(runtime);
                    }
                    runtime.Send(new IEventHandle(sub.ActorId, edge, new TEvents::TEvPoison), 0, true);
                    sub.Alive = false;
                    Dispatch(runtime);
                }
                break;
            }

            case 6:
                CheckSchedulerInvariant(subscribers);
                break;
        }
    }

    for (auto& sub : subscribers) {
        if (sub.Alive && sub.Registered) {
            RegisterActorInUniversalScheduler(sub.ActorId, nullptr, runtime.GetActorSystem(0));
            sub.Registered = false;
            Dispatch(runtime);
        }
    }
    runtime.Send(new IEventHandle(scheduler, edge, new TEvents::TEvPoison), 0, true);
    Dispatch(runtime);
}

void CheckSchedulerInvariant(const TVector<TSubscriber>& subscribers) {
    for (const auto& sub : subscribers) {
        if (!sub.Alive) {
            Y_ABORT_UNLESS(!sub.Registered);
        }
        Y_ABORT_UNLESS(sub.FlowRecord);
        Y_ABORT_UNLESS(sub.FlowRecord->GetPredictedDelayNs() >= 0);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    RunUnischedActorFuzz(fdp);
    return 0;
}
