#include "cli.h"
#include "melancholic_gopher.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/executor_pool_basic.h>

namespace NKikimr {
namespace NDriverClient {

struct TCmdActorsysPerfConfig : public TCliCmdConfig {
    ui32 Threads = 3;
    ui32 Duration = 60;

    void Parse(int argc, char **argv) {
        using namespace NLastGetopt;
        TOpts opts = TOpts::Default();
        opts.AddLongOption('t', "threads", "size of thread pool to measure").DefaultValue(3).StoreResult(&Threads);
        opts.AddLongOption('d', "duration", "duration of test in seconds").DefaultValue(60).StoreResult(&Duration);

        ConfigureBaseLastGetopt(opts);
        TOptsParseResult res(&opts, argc, argv);
    }
};

int ActorsysPerfTest(TCommandConfig &cmdConf, int argc, char **argv) {
    Y_UNUSED(cmdConf);
    using namespace NActors;

    TCmdActorsysPerfConfig config;
    config.Parse(argc, argv);

    if (config.Threads < 1 || config.Threads > 1000) {
        Cerr << "Thread pool size must be in [1, 1000] range";
        return EXIT_FAILURE;
    }

    THolder<TActorSystemSetup> setup(new TActorSystemSetup());
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, config.Threads, 0));
    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(1024, 0, 100000, false)));

    Cerr << "Starting test for " << TDuration::Seconds(config.Duration).ToString() << " with " << config.Threads << " threads" << Endl;

    TActorSystem actorSys(setup, nullptr);
    TVector<TExecutorThreadStats> stats(1);
    TVector<TExecutorThreadStats> sharedStats;
    TExecutorPoolStats poolStats;

    TVector<std::pair<ui32, double>> lineProfile = {{ 0, .0 }, { 0, .0 }, { 0, .0 }, { 0, .0 }, { 0, .0 }, { 0, .0 }, { 0, 0 }, { 0, .0 }};

    actorSys.Start();
    actorSys.Register(CreateGopherMother(lineProfile, 1000, 2));
    Sleep(TDuration::Seconds(config.Duration));
    actorSys.GetPoolStats(0, poolStats, stats, sharedStats);
    actorSys.Stop();

    ui64 sentEvents = 0;
    for (auto &x : stats)
        sentEvents += x.SentEvents;
    for (auto &x : sharedStats)
        sentEvents += x.SentEvents;

    Cerr << "Produced " << sentEvents << " signals at rate " << sentEvents/config.Duration << " per second " << sentEvents/config.Duration/config.Threads << " per thread" << Endl;

    return EXIT_SUCCESS;
}

}
}
