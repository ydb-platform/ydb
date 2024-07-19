#include "query_replay.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/should_continue.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/generic/xrange.h>


void TQueryReplayConfig::ParseConfig(int argc, const char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    opts.AddLongOption("cluster", "YT cluster").StoreResult(&Cluster);
    opts.AddLongOption("src-path", "Source table path").StoreResult(&SrcPath);
    opts.AddLongOption("dst-path", "Target table path").StoreResult(&DstPath);
    opts.AddLongOption("threads", "Number of ActorSystem threads").StoreResult(&ActorSystemThreadsCount);
    opts.AddLongOption("udf-file", "UDFS to load").AppendTo(&UdfFiles);
    opts.AddLongOption("query", "Single query to replay").StoreResult(&QueryFile);
    opts.AddLongOption("log-level", "Yql log level").StoreResult(&YqlLogLevel);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);
}

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
