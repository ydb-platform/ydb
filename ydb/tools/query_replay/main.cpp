#include "query_replay.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/should_continue.h>
#include <util/system/sigset.h>
#include <util/generic/xrange.h>
#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/folder/pathsplit.h>
#include <util/string/strip.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>

using namespace NActors;

static TProgramShouldContinue ShouldContinue;

void OnTerminate(int) {
    ShouldContinue.ShouldStop();
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

int TQueryReplayApp::ParseConfig(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    TString authPath;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").RequiredArgument("HOST:PORT").StoreResult(&Endpoint);
    opts.AddLongOption('d', "database", "YDB database name").RequiredArgument("PATH").StoreResult(&Database);
    opts.AddLongOption('p', "path", "Target table").RequiredArgument("PATH").StoreResult(&Path);
    opts.AddLongOption("stats-path", "Stats table").RequiredArgument("PATH").StoreResult(&StatsPath);
    opts.AddLongOption('i', "in-flight", "Number of queries compiling concurrently").RequiredArgument("NUM").StoreResult(&MaxInFlight);
    opts.AddLongOption('m', "modulo", "Number of queries compiling concurrently").RequiredArgument("NUM").StoreResult(&Modulo);
    opts.AddLongOption('s', "shard-id", "Shard id").RequiredArgument("NUM").StoreResult(&ShardId);
    opts.AddLongOption('t', "threads", "Number of ActorSystem threads").RequiredArgument("NUM").StoreResult(&ActorSystemThreadsCount);
    opts.AddLongOption("auth", "Auth token file").RequiredArgument("PATH").StoreResult(&authPath);
    opts.AddLongOption('q', "query", "Explicit query to replay").RequiredArgument("PATH").AppendTo(&Queries);
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    if (authPath) {
        TAutoPtr<TMappedFileInput> fileInput(new TMappedFileInput(authPath));
        AuthToken = Strip(fileInput->ReadAll());
    } else {
        AuthToken = Strip(GetEnv("YDB_TOKEN"));
    }

    DriverConfig = NYdb::TDriverConfig()
                       .SetEndpoint(Endpoint)
                       .SetDatabase(Database);

    if (AuthToken)
        DriverConfig.SetAuthToken(AuthToken);

    if (!Path.empty() && Path.front() != '/')
        Path = Database + "/" + Path;

    if (!StatsPath.empty() && StatsPath.front() != '/') {
        StatsPath = Database + "/" + StatsPath;
    }

    return 0;
}

void TQueryReplayApp::Start() {
    QueryReplayStats.reset(new TQueryReplayStats());
    RandomProvider = CreateDefaultRandomProvider();
    InitializeLogger();
    THolder<TActorSystemSetup> setup = BuildActorSystemSetup(ActorSystemThreadsCount, 1);
    TypeRegistry.Reset(new NKikimr::NScheme::TKikimrTypeRegistry());
    FunctionRegistry.Reset(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone());
    NKikimr::NMiniKQL::FillStaticModules(*FunctionRegistry);
    AppData.Reset(new NKikimr::TAppData(0, 0, 0, 0, {}, TypeRegistry.Get(), FunctionRegistry.Get(), nullptr, nullptr));
    AppData->Counters = MakeIntrusive<NMonitoring::TDynamicCounters>(new NMonitoring::TDynamicCounters());
    ActorSystem.Reset(new TActorSystem(setup, AppData.Get(), LogSettings));
    ActorSystem->Start();
    ActorSystem->Register(NKikimr::NKqp::CreateKqpResourceManagerActor({}, nullptr));

    if (Queries.empty()) {
        const auto runId = RandomProvider->GenUuid4().AsUuidString();
        Cout << "Starting replay over table " << Path << ", runId: " << runId << Endl;
        Driver = std::make_unique<NYdb::TDriver>(DriverConfig);
        ReplayActor = ActorSystem->Register(CreateQueryReplayActor(*Driver, runId, Path, StatsPath, QueryReplayStats, FunctionRegistry.Get(), MaxInFlight, Modulo, ShardId));
    } else {
        Cout << "Starting local replay, query ids count: " << Queries.size() << Endl;
        ReplayActor = ActorSystem->Register(CreateQueryReplayActorSimple(std::move(Queries), QueryReplayStats, FunctionRegistry.Get()));
    }
}

void TQueryReplayApp::Stop() {
    ActorSystem->Stop();
    if (Driver) {
        Driver->Stop(true);
    }
}

void TQueryReplayApp::InitializeLogger() {
    TActorId loggerActorId = TActorId(1, "logger");
    LogSettings.Reset(new NLog::TSettings(loggerActorId, NActorsServices::LOGGER,
                                          NLog::EPriority::PRI_DEBUG, NLog::EPriority::PRI_DEBUG, 0));

    LogSettings->Append(
        NKikimrServices::EServiceKikimr_MIN,
        NKikimrServices::EServiceKikimr_MAX,
        NKikimrServices::EServiceKikimr_Name);
    LogSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name);

    TString expl;
    LogSettings->SetLevel(NLog::EPriority::PRI_DEBUG, NKikimrServices::GRPC_SERVER, expl);
    LogBackend.Reset(NActors::CreateStderrBackend().Release());
}

int main(int argc, char** argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);

#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    TQueryReplayApp app;
    if (int parseRet = app.ParseConfig(argc, argv)) {
        Cerr << "Exit: " << parseRet << Endl;
        return parseRet;
    }
    app.Start();
    ui32 idx = 0;
    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        if (app.QueryReplayStats->IsComplete()) {
            ShouldContinue.ShouldStop();
            break;
        }

        ++idx;
        if (idx % 60 == 0) {
            Cout << "Completed " << app.QueryReplayStats->GetCompletedCount() << " out of " << app.QueryReplayStats->GetTotalCount() << Endl;
        }

        Sleep(TDuration::MilliSeconds(200));
    }

    app.Stop();
    Cout << "Total run queries: " << app.QueryReplayStats->GetTotalCount() << Endl;
    Cout << "Successful queries: " << app.QueryReplayStats->GetCompletedCount() << Endl;
    Cout << "Failed queries: " << app.QueryReplayStats->GetErrorsCount() << Endl;

    return (ShouldContinue.GetReturnCode() != 0 || app.QueryReplayStats->GetErrorsCount() > 0) ? 1 : 0;
}
