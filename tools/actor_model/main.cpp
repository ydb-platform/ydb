#include "actors.h"
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <util/generic/xrange.h>
#include <limits>
#include "theSieveOfEratosthenes.h"
THolder<NActors::TActorSystemSetup> BuildActorSystemSetup(ui32 threads, ui32 pools) {
    auto setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->ExecutorsCount = pools;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[pools]);
    for (ui32 idx : xrange(pools)) {
        setup->Executors[idx] = new NActors::TBasicExecutorPool(idx, threads, 512);
    }
    setup->Scheduler.Reset(new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0)));
    return setup;
}


int main(int argc, const char* argv[])
{
    Y_UNUSED(argc, argv);
    auto actorySystemSetup = BuildActorSystemSetup(20, 1);
    NActors::TActorSystem actorSystem(actorySystemSetup);
    actorSystem.Start();

    actorSystem.Register(CreateSelfPingActor(TDuration::Seconds(1)).Release());

    // Зарегистрируйте Write и Read акторы здесь
    NActors::TActorId writeActorID = actorSystem.Register(CreateSelfTWriteActor().Release());
    actorSystem.Register(CreateSelfTReadActor(writeActorID, new TheSieveOfEratosthenes()).Release());

    // Раскомментируйте этот код
    auto shouldContinue = GetProgramShouldContinue();
    while (shouldContinue->PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(200));
    }
    actorSystem.Stop();
    actorSystem.Cleanup();
    return shouldContinue->GetReturnCode();
}