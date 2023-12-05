#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/should_continue.h>
#include <util/system/sigset.h>
#include <util/generic/xrange.h>

using namespace NActors;

static TProgramShouldContinue ShouldContinue;

void OnTerminate(int) {
    ShouldContinue.ShouldStop();
}

class TPingActor : public TActorBootstrapped<TPingActor> {
    const TActorId Target;
    ui64 HandledEvents;
    TInstant PeriodStart;

    void Handle(TEvents::TEvPing::TPtr &ev) {
        Send(ev->Sender, new TEvents::TEvPong());
        Send(ev->Sender, new TEvents::TEvPing());
        Become(&TThis::StatePing);
    }

    void Handle(TEvents::TEvPong::TPtr &ev) {
        Y_UNUSED(ev);
        Become(&TThis::StateWait);
    }

    void PrintStats() {
        const i64 ms = (TInstant::Now() - PeriodStart).MilliSeconds();
        Cout << "Handled " << 2 * HandledEvents << " over " << ms << "ms" << Endl;
        ScheduleStats();
    }

    void ScheduleStats() {
        HandledEvents = 0;
        PeriodStart = TInstant::Now();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

public:
    TPingActor(TActorId target)
        : Target(target)
        , HandledEvents(0)
        , PeriodStart(TInstant::Now())
    {}

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPing, Handle);
            sFunc(TEvents::TEvWakeup, PrintStats);
        }

        ++HandledEvents;
    }

    STFUNC(StatePing) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvPong, Handle);
            sFunc(TEvents::TEvWakeup, PrintStats);
        }

        ++HandledEvents;
    }

    void Bootstrap() {
        if (Target) {
            Become(&TThis::StatePing);
            Send(Target, new TEvents::TEvPing());
            ScheduleStats();
        }
        else {
            Become(&TThis::StateWait);
        };
    }
};

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 threads, ui32 pools) {
    Y_ABORT_UNLESS(threads > 0 && threads < 100);
    Y_ABORT_UNLESS(pools > 0 && pools < 10);

    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = 1;

    setup->ExecutorsCount = pools;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[pools]);
    for (ui32 idx : xrange(pools)) {
        setup->Executors[idx] = new TBasicExecutorPool(idx, threads, 50);
    }

    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

int main(int argc, char **argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);

#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    THolder<TActorSystemSetup> actorSystemSetup = BuildActorSystemSetup(2, 1);
    TActorSystem actorSystem(actorSystemSetup);

    actorSystem.Start();

    const TActorId a = actorSystem.Register(new TPingActor(TActorId()));
    const TActorId b = actorSystem.Register(new TPingActor(a));
    Y_UNUSED(b);

    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(200));
    }

    actorSystem.Stop();
    actorSystem.Cleanup();

    return ShouldContinue.GetReturnCode();
}
