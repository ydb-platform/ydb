#pragma once

#include "objectwithstate.h"
#include "state_manager.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <library/cpp/lwtrace/all.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>
#include <util/random/fast.h>

#include <sys/wait.h>

class TFailInjector {
    TAtomic FailCounter = 0;
    TAutoEvent FailEvent;
    TInstant Deadline = TInstant::Max();

public:
    void SetDeadline(TInstant deadline) {
        Deadline = deadline;
    }

    void SetFailCounter(ui32 failCounter) {
        AtomicSet(FailCounter, failCounter);
    }

    void Inject(ui64 cookie) {
        if (cookie == 2 && RandomNumber(10u) == 0) {
            AtomicSet(FailCounter, 1 + RandomNumber(24u));
        }

        TAtomicBase result = AtomicDecrement(FailCounter);
        if (result < 0) {
            // overshoot, return one position back
            AtomicIncrement(FailCounter);
        } else if (result == 0) {
            // just came to zero, signal fail event
            FailEvent.Signal();
        }

        // block this thread indefinitely if fail has arrived
        if (result <= 0) {
            BlockThread();
        }
    }

    void WaitForFailure() {
        FailEvent.WaitD(Deadline);
    }

private:
    static void BlockThread() {
        // just sleep for one second, and then sleep again :-)
        for (;;) {
            NanoSleep(1000 * 1000 * 1000);
        }
    }
};

class TFailInjectionActionExecutor : public NLWTrace::TCustomActionExecutor {
    TFailInjector *Injector;

public:
    TFailInjectionActionExecutor(NLWTrace::TProbe *probe, TFailInjector *injector)
        : NLWTrace::TCustomActionExecutor(probe, true /* destructive */)
        , Injector(injector)
    {}

private:
    bool DoExecute(NLWTrace::TOrbit&, const NLWTrace::TParams& params) override {
        Injector->Inject(params.Param[0].Get<ui64>());
        return true;
    }
};

class TFailCounterGenerator {

};

ui32 GenerateFailCounter(bool frequentFails) {
    TReallyFastRng32 rng(Now().GetValue());

    double p = (rng() % (1000 * 1000 * 1000)) / 1e9;

    if (!frequentFails) {
        return p < 0.05 ? rng() % 10 + 1
            : p < 0.10 ? rng() % 100 + 100 :
            rng() % 1000 + 1000;
    } else {
        return p < 0.9 ? rng() % 10 + 1
            : p < 0.99 ? rng() % 100 + 100 :
            rng() % 1000 + 1000;
    }
}

struct TPDiskFailureInjectionTest {
    // default values for unit test
    ui32 NumIterations = 10; // 0 = unlimited
    ui32 NumFailsInIteration = 1000; // 0 = unlimited
    TTempDir TempDir;
    TString PDiskFilePath;
    ui64 DiskSize = 16ULL << 30; // 10 GB
    ui32 ChunkSize = 16 << 20; // 16 MB
    ui32 SectorSize = 4 << 10; // 4 KB
    ui64 PDiskGuid;
    bool ErasureEncode = false;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TProgramShouldContinue KikimrShouldContinue;
    std::unique_ptr<NKikimr::TAppData> AppData;
    std::shared_ptr<NKikimr::NPDisk::IIoContextFactory> IoContext;
    std::unique_ptr<NActors::TActorSystem> ActorSystem;

    TAutoEvent StopEvent;

    NLWTrace::TManager TraceManager;

    TMaybe<TDuration> TestDuration;

    TPDiskFailureInjectionTest()
        : TraceManager(*Singleton<NLWTrace::TProbeRegistry>(), true)
    {}

    void Setup() {
        // generate temporary pdisk file name if not set
        if (!PDiskFilePath) {
            PDiskFilePath = TempDir() + "pdisk.bin";
        }

        // generate random guid
        PDiskGuid = Now().GetValue();

        // format pdisk
        NKikimr::FormatPDisk(PDiskFilePath, DiskSize, SectorSize, ChunkSize, PDiskGuid, 1, 1, 1, 1, "text message",
                ErasureEncode, false, nullptr, false);

        Cerr << "created pdisk at " << PDiskFilePath << Endl;
    }

    void SetupLWTrace(TFailInjector *injector) {
        const char *name = "FailInjectionAction";

        NLWTrace::TQuery query;
        auto& block = *query.AddBlocks();
        auto& desc = *block.MutableProbeDesc();
        desc.SetName("PDiskFailInjection");
        desc.SetProvider("FAIL_INJECTION_PROVIDER");
        auto& action = *block.AddAction();
        auto& custom = *action.MutableCustomAction();
        custom.SetName(name);

        auto factory = [=](NLWTrace::TProbe *probe, const NLWTrace::TCustomAction& /*action*/, NLWTrace::TSession* /*session*/) {
            return new TFailInjectionActionExecutor(probe, injector);
        };

        TraceManager.RegisterCustomAction(name, factory);
        TraceManager.New("env", query);
    }

    void InitActorSystem() {
        using namespace NActors;

        // create counters
        Counters = new ::NMonitoring::TDynamicCounters;

        // initialize app data with pool ids and registries
        AppData.reset(new NKikimr::TAppData(0u, 1u, 2u, 3u, {}, nullptr, nullptr, nullptr, &KikimrShouldContinue));
        IoContext = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
        AppData->IoContextFactory = IoContext.get();

        // create actor system setup environment
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 4; // system, user, io, batch
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new TBasicExecutorPool(AppData->SystemPoolId, 16, 10);
        setup->Executors[1] = new TBasicExecutorPool(AppData->UserPoolId, 1, 10);
        setup->Executors[2] = new TIOExecutorPool(AppData->IOPoolId, 10);
        setup->Executors[3] = new TBasicExecutorPool(AppData->BatchPoolId, 1, 10);
        setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 100));

        // initialize logger settings
        const TActorId loggerId(setup->NodeId, "logger");

        TIntrusivePtr<NLog::TSettings> loggerSettings = new NLog::TSettings(loggerId, NActorsServices::LOGGER,
                NActors::NLog::PRI_NOTICE, NActors::NLog::PRI_DEBUG, 0);

        loggerSettings->Append(
            NActorsServices::EServiceCommon_MIN,
            NActorsServices::EServiceCommon_MAX,
            NActorsServices::EServiceCommon_Name
        );

        loggerSettings->Append(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name
        );

        TString explanation;
        loggerSettings->SetLevel(NActors::NLog::PRI_INFO, NKikimrServices::BS_PDISK, explanation);
        loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NKikimrServices::BS_PDISK_TEST, explanation);

        // create/register logger actor
        auto logger = std::make_unique<TLoggerActor>(loggerSettings, CreateStderrBackend(),
                Counters->GetSubgroup("logger", "counters"));
        setup->LocalServices.emplace_back(loggerId, TActorSetupCmd(logger.release(), TMailboxType::Simple, AppData->IOPoolId));

        // create and then initialize actor system
        ActorSystem = std::make_unique<TActorSystem>(setup, AppData.get(), loggerSettings);
    }

    template<typename TTest>
    void Run(TTest *test, TStateManager *stateManager, const TString& data = {}) {
        TObjectWithState::DeserializeCommonState(data);
        InitActorSystem();
        ActorSystem->Start();
        LOG_NOTICE(*ActorSystem, NActorsServices::TEST, "actor system started");
        test->Run(this, &StopEvent, stateManager);
    }

    //template<bool FREQUENT_FAILS, typename TTest, typename... TArgs>
    template<typename TTest, typename... TArgs>
    void RunCycle(bool frequentFails, TArgs&&... args) {
        TInstant startTime = TInstant::Now();

        for (ui32 iteration = 0; NumIterations == 0 || iteration < NumIterations; ++iteration) {
            Cerr << "iteration# " << iteration << Endl;

            // reset pdisk to initial state
            Setup();

            // stored common state
            TString state;

            for (ui32 fail = 0; NumFailsInIteration == 0 || fail < NumFailsInIteration; ++fail) {
                if (TestDuration && TInstant::Now() >= startTime + *TestDuration) {
                    return;
                }

                Cerr << "iteration# " << iteration << " fail# " << fail << " stateLen# " << state.size() << Endl;

                int fds[2];
                if (pipe(fds) != 0) {
                    Y_ABORT("pipe failed");
                }

                pid_t pid = fork();
                if (pid == 0) {
                    // close read end of pipe
                    close(fds[0]);

                    TFailInjector injector;

                    TReallyFastRng32 rng(Now().GetValue());

                    ui32 failCounter = GenerateFailCounter(frequentFails);
                    injector.SetFailCounter(failCounter);
                    if (TestDuration) {
                        injector.SetDeadline(startTime + *TestDuration);
                    }

                    Cerr << "failCounter# " << failCounter << Endl;

                    SetupLWTrace(&injector);
                    TStateManager stateManager;
                    Run(new TTest(TArgs(args)...), &stateManager, state);
                    injector.WaitForFailure();
                    stateManager.StopRunning();

                    // store common state into variable...
                    state = TObjectWithState::SerializeCommonState();

                    // ...and move it through pipe
                    size_t pos = 0;
                    while (pos < state.size()) {
                        ssize_t written = write(fds[1], state.data() + pos, state.size() - pos);
                        if (written == -1) {
                            if (errno == EINTR) {
                                continue;
                            } else {
                                perror("write to pipe failed");
                                abort();
                            }
                        } else {
                            pos += written;
                        }
                    }

                    // close write end of pipe
                    close(fds[1]);

                    // terminate
                    raise(SIGKILL);
                } else if (pid > 0) {
                    Cerr << "pid# " << pid << Endl;

                    // close write end of pipe
                    close(fds[1]);

                    // read buffer from pipe
                    state.clear();
                    for (;;) {
                        char buffer[4096];
                        ssize_t len = read(fds[0], buffer, sizeof(buffer));
                        if (len == -1) {
                            if (errno != EINTR) {
                                Y_ABORT("unexpected error: %s", strerror(errno));
                            }
                            continue;
                        } else if (!len) {
                            break;
                        } else {
                            state.append(buffer, len);
                        }
                    }

                    // close read end of pipe
                    close(fds[0]);

                    // wait for child to terminate
                    int status = 0;
                    if (waitpid(pid, &status, 0) != pid) {
                        Y_ABORT("waitpid failed with error: %s", strerror(errno));
                    }

                    if (WIFSIGNALED(status)) {
                        int sig = WTERMSIG(status);
                        if (sig != SIGKILL) {
                            Y_ABORT("unexpected termination signal: %d pid# %d", sig, (int)pid);
                        }
                    }
                } else {
                    perror("failed to fork");
                }
            }
        }
    }
};
