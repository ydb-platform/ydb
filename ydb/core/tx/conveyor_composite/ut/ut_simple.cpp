#include <library/cpp/retry/retry.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/signals/object_counter.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>

#include <array>
#include <functional>

using namespace NKikimr::NConveyorComposite;

namespace NKikimr {

THolder<TActorSystemSetup> BuildActorSystemSetup(const ui32 threads, const ui32 pools) {
    Y_ABORT_UNLESS(threads > 0 && threads < 100);
    Y_ABORT_UNLESS(pools > 0 && pools < 10);

    auto setup = MakeHolder<NActors::TActorSystemSetup>();

    setup->NodeId = 1;

    setup->ExecutorsCount = pools;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[pools]);
    for (ui32 idx : xrange(pools)) {
        setup->Executors[idx] = new NActors::TBasicExecutorPool(idx, threads, 50);
    }

    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 0));

    return setup;
}

}   // namespace NKikimr

class TSleepTask: public NKikimr::NConveyor::ITask {
private:
    const TDuration ExecutionTime;
    TAtomicCounter* Counter;
    TAtomicCounter* Accounted = nullptr;
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        const TMonotonic start = TMonotonic::Now();
        while (TMonotonic::Now() - start < ExecutionTime) {
        }
        Counter->Inc();
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "SLEEP";
    }

    virtual std::function<void()> MakeAccountedCallback() const override {
        if (!Accounted) {
            return {};
        }
        return [c = Accounted]() {
            c->Inc();
        };
    }

    TSleepTask(const TDuration d, TAtomicCounter& c, TAtomicCounter* accounted = nullptr)
        : ExecutionTime(d)
        , Counter(&c)
        , Accounted(accounted) {
    }
};

class TWorkerRecordingTask: public NKikimr::NConveyor::ITask {
private:
    const TDuration ExecutionTime;
    TAtomicCounter* Counter;
    std::array<TAtomicCounter, 16>* PerWorker = nullptr;
    ui64 AssignedWorker = Max<ui64>();

    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        const TMonotonic start = TMonotonic::Now();
        while (TMonotonic::Now() - start < ExecutionTime) {
        }
        if (PerWorker) {
            AFL_VERIFY(AssignedWorker < PerWorker->size())("worker", AssignedWorker);
            (*PerWorker)[AssignedWorker].Inc();
        }
        Counter->Inc();
    }

public:
    virtual TString GetTaskClassIdentifier() const override {
        return "SLEEP_RECORDING";
    }

    virtual void OnAssignedToWorker(const ui64 workerIdx) override {
        AssignedWorker = workerIdx;
    }

    TWorkerRecordingTask(const TDuration d, TAtomicCounter& c, std::array<TAtomicCounter, 16>* perWorker)
        : ExecutionTime(d)
        , Counter(&c)
        , PerWorker(perWorker) {
    }
};

void WaitCounter(TAtomicCounter& counter, const i64 expected) {
    const TMonotonic deadline = TMonotonic::Now() + TDuration::Seconds(30);
    while (counter.Val() < expected) {
        UNIT_ASSERT_C(TMonotonic::Now() < deadline, "timeout waiting for conveyor tasks");
        Sleep(TDuration::MilliSeconds(10));
    }
    UNIT_ASSERT_VALUES_EQUAL(counter.Val(), expected);
}

void WaitWarmupAccounted(NActors::TActorSystem& actorSystem, const NActors::TActorId& actorId, const ESpecialTaskCategory category,
    const ui64 processId, const ui32 warmupTasks, const TDuration warmup) {
    TAtomicCounter warmupDone;
    TAtomicCounter warmupAccounted;
    for (ui32 i = 0; i < warmupTasks; ++i) {
        actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
            std::make_shared<TSleepTask>(warmup, warmupDone, &warmupAccounted), category, processId));
    }
    WaitCounter(warmupAccounted, warmupTasks);
}

NConfig::TConfig ParseConveyorProto(const TString& textProto) {
    NKikimrConfig::TCompositeConveyorConfig protoConfig;
    AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));
    return NConfig::TConfig::BuildFromProto(protoConfig).DetachResult();
}

class IRequestProcessor {
private:
    YDB_READONLY_DEF(TString, Id);
    virtual void DoInitialize(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) = 0;
    virtual void DoAddTask(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) = 0;
    virtual bool DoCheckFinished() = 0;
    virtual void DoFinish(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId, const TDuration d) = 0;
    virtual TString DoDebugString() const = 0;

public:
    void Initialize(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) {
        DoInitialize(actorSystem, distributorId);
    }
    void AddTask(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) {
        DoAddTask(actorSystem, distributorId);
    }
    bool CheckFinished() {
        return DoCheckFinished();
    }
    void Finish(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId, const TDuration d) {
        DoFinish(actorSystem, distributorId, d);
    }
    TString DebugString() const {
        return TStringBuilder() << "{" << Id << ":" << DoDebugString() << "}";
    }
    IRequestProcessor(const TString& id)
        : Id(id) {
    }
    virtual ~IRequestProcessor() = default;
};

class TSimpleRequest: public IRequestProcessor {
private:
    YDB_ACCESSOR(double, ScopeWeight, 1);
    const ESpecialTaskCategory Category;
    const TString ScopeId;
    const ui64 ProcessId;
    TAtomicCounter Counter;
    TAtomicCounter CounterTasks;
    virtual TString DoDebugString() const override {
        return TStringBuilder() << Counter.Val();
    }

    virtual void DoInitialize(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) override {
        actorSystem.Send(distributorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000, ScopeWeight), Category, ScopeId, ProcessId));
    }
    virtual void DoAddTask(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId) override {
        actorSystem.Send(distributorId,
            new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter), Category, ProcessId));
        CounterTasks.Inc();
    }
    virtual bool DoCheckFinished() override {
        return CounterTasks.Val() == Counter.Val();
    }
    virtual void DoFinish(NActors::TActorSystem& actorSystem, const NActors::TActorId distributorId, const TDuration /*d*/) override {
        actorSystem.Send(distributorId, new TEvExecution::TEvUnregisterProcess(Category, ProcessId));
    }

public:
    TSimpleRequest(const TString& id, const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId)
        : IRequestProcessor(id)
        , Category(category)
        , ScopeId(scopeId)
        , ProcessId(processId) {
    }
};

class TTestingExecutor {
private:
    virtual TString GetConveyorConfig() = 0;
    virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() = 0;
    virtual ui32 GetTasksCount() const {
        return 1000000;
    }

public:
    virtual double GetThreadsCount() const {
        return 9.5;
    }

    void Execute() {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);

        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const std::string textProto = GetConveyorConfig();
        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));

        NConfig::TConfig config = NConfig::TConfig::BuildFromProto(protoConfig).DetachResult();
        const auto actorId = actorSystem.Register(CreateService(config, counters));

        std::vector<std::shared_ptr<IRequestProcessor>> requests = GetRequests();
        for (auto&& i : requests) {
            i->Initialize(actorSystem, actorId);
        }
        for (ui32 i = 0; i < GetTasksCount(); ++i) {
            for (auto&& i : requests) {
                i->AddTask(actorSystem, actorId);
            }
        }
        const TMonotonic globalStart = TMonotonic::Now();
        std::vector<TDuration> durations;
        durations.resize(requests.size());
        {
            bool isFinished = false;
            while (!isFinished) {
                isFinished = true;
                ui32 idx = 0;
                TStringBuilder sb;
                for (auto&& i : requests) {
                    if (!i->CheckFinished()) {
                        isFinished = false;
                    } else if (!durations[idx]) {
                        durations[idx] = TMonotonic::Now() - globalStart;
                    }
                    sb << i->DebugString() << ";";
                    ++idx;
                }
                Cerr << sb << Endl;
                if (!isFinished) {
                    Sleep(TDuration::Seconds(1));
                }
            }
        }
        {
            ui32 idx = 0;
            for (auto&& i : requests) {
                i->Finish(actorSystem, actorId, durations[idx]);
                ++idx;
            }
        }
        Cerr << (GetThreadsCount() * (TMonotonic::Now() - globalStart) / (1.0 * requests.size() * GetTasksCount())).MicroSeconds()
             << "us per task" << Endl;
        TStringBuilder sb;
        for (auto&& i : durations) {
            sb << i << ";";
        }
        Cerr << sb << Endl;

        int expected = 5;
        ui32 retries = 60;
        auto sleep = TDuration::Seconds(1);
        auto getCount = []() {
            return NKikimr::NColumnShard::TMonitoringObjectsCounter<TProcessScope>::GetCounter().Val();
        };
        auto checkCount = [&]() {
            return getCount() == expected;
        };

        bool result = DoWithRetryOnRetCode(checkCount, TRetryOptions{retries, sleep});
        AFL_VERIFY(result)("count", getCount());

        actorSystem.Stop();
        actorSystem.Cleanup();
    };
};

Y_UNIT_TEST_SUITE(CompositeConveyorTests) {
    Y_UNIT_TEST(ProcessGuardMovePreservesProcessState) {
        NActors::TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        const auto serviceId = TServiceOperator::MakeServiceId(runtime.GetNodeId(0));
        const auto serviceEdge = runtime.AllocateEdgeActor();
        runtime.RegisterService(serviceId, serviceEdge);

        THashSet<ui64> registrations;
        THashSet<ui64> tasks;
        THashSet<ui64> unregistrations;
        auto registrationObserver = runtime.AddObserver<TEvExecution::TEvRegisterProcess>([&](auto& ev) {
            if (ev->Recipient == serviceId) {
                registrations.emplace(ev->Get()->GetInternalProcessId());
            }
        });
        auto taskObserver = runtime.AddObserver<TEvExecution::TEvNewTask>([&](auto& ev) {
            if (ev->Recipient == serviceId) {
                tasks.emplace(ev->Get()->GetInternalProcessId());
            }
        });
        auto unregistrationObserver = runtime.AddObserver<TEvExecution::TEvUnregisterProcess>([&](auto& ev) {
            if (ev->Recipient == serviceId) {
                unregistrations.emplace(ev->Get()->GetInternalProcessId());
            }
        });

        TAtomicCounter taskCounter;
        ui64 activeProcessId = 0;
        ui64 finishedProcessId = 0;
        UNIT_ASSERT(runtime.RunCall([&] {
            {
                TProcessGuard guard(ESpecialTaskCategory::Scan, "active", 1, TCPULimitsConfig(), serviceId);
                activeProcessId = guard.GetInternalProcessId();

                TProcessGuard movedGuard(std::move(guard));
                UNIT_ASSERT_VALUES_EQUAL(movedGuard.GetInternalProcessId(), activeProcessId);
                movedGuard.SendTaskToExecute(std::make_shared<TSleepTask>(TDuration::Zero(), taskCounter));
                movedGuard.Finish();
            }
            {
                TProcessGuard guard(ESpecialTaskCategory::Scan, "finished", 2, TCPULimitsConfig(), serviceId);
                finishedProcessId = guard.GetInternalProcessId();
                guard.Finish();

                TProcessGuard movedGuard(std::move(guard));
                UNIT_ASSERT_VALUES_EQUAL(movedGuard.GetInternalProcessId(), finishedProcessId);
            }
            NActors::TActorContext::AsActorContext().Send(serviceEdge, new NActors::TEvents::TEvWakeup());
            return true;
        }));

        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(serviceEdge);

        UNIT_ASSERT_VALUES_EQUAL(registrations.size(), 2);
        UNIT_ASSERT(registrations.contains(activeProcessId));
        UNIT_ASSERT(registrations.contains(finishedProcessId));
        UNIT_ASSERT_VALUES_EQUAL(tasks.size(), 1);
        UNIT_ASSERT(tasks.contains(activeProcessId));
        UNIT_ASSERT_VALUES_EQUAL(unregistrations.size(), 2);
        UNIT_ASSERT(unregistrations.contains(activeProcessId));
        UNIT_ASSERT(unregistrations.contains(finishedProcessId));
    }

    class TTestingExecutor10xDistribution: public TTestingExecutor {
    private:
        virtual TString GetConveyorConfig() override {
            return Sprintf(R"(
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "insert"
                        Weight: 0.1
                    }
                    Links {
                        Category: "scan"
                        Weight: 0.01
                    }
                    Links {
                        Category: "normalizer"
                        Weight: 0.001
                    }
                }
                Categories {
                    Name: "insert"
                }
                Categories {
                    Name: "normalizer"
                }
                Categories {
                    Name: "scan"
                }
                )",
                GetThreadsCount());
        }
        virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() override {
            return { std::make_shared<TSimpleRequest>("I", ESpecialTaskCategory::Insert, "1", 1),
                std::make_shared<TSimpleRequest>("S", ESpecialTaskCategory::Scan, "1", 1),
                std::make_shared<TSimpleRequest>("N", ESpecialTaskCategory::Normalizer, "1", 1) };
        }

    public:
    };
    Y_UNIT_TEST(Test10xDistribution) {
        TTestingExecutor10xDistribution().Execute();
    }

    class TTestingExecutor10xMultiDistribution: public TTestingExecutor {
    private:
        virtual TString GetConveyorConfig() override {
            return Sprintf(R"(
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "scan"
                        Weight: 1
                    }
                }
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "insert"
                        Weight: 0.1
                    }
                    Links {
                        Category: "scan"
                        Weight: 0.01
                    }
                    Links {
                        Category: "normalizer"
                        Weight: 0.001
                    }
                }
                Categories {
                    Name: "insert"
                }
                Categories {
                    Name: "normalizer"
                }
                Categories {
                    Name: "scan"
                }
                )",
                GetThreadsCount(), GetThreadsCount());
        }
        virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() override {
            return { std::make_shared<TSimpleRequest>("I", ESpecialTaskCategory::Insert, "1", 1),
                std::make_shared<TSimpleRequest>("S", ESpecialTaskCategory::Scan, "1", 1),
                std::make_shared<TSimpleRequest>("N", ESpecialTaskCategory::Normalizer, "1", 1) };
        }

    public:
    };
    Y_UNIT_TEST(Test10xMultiDistribution) {
        TTestingExecutor10xMultiDistribution().Execute();
    }

    class TTestingExecutorUniformProcessDistribution: public TTestingExecutor {
    private:
        virtual TString GetConveyorConfig() override {
            return Sprintf(R"(
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "insert"
                        Weight: 1
                    }
                }
                Categories {
                    Name: "insert"
                }
                )",
                GetThreadsCount());
        }
        virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() override {
            return { std::make_shared<TSimpleRequest>("1", ESpecialTaskCategory::Insert, "1", 1),
                std::make_shared<TSimpleRequest>("2", ESpecialTaskCategory::Insert, "1", 2),
                std::make_shared<TSimpleRequest>("3", ESpecialTaskCategory::Insert, "1", 3) };
        }

    public:
    };
    Y_UNIT_TEST(TestUniformProcessDistribution) {
        TTestingExecutorUniformProcessDistribution().Execute();
    }

    class TTestingExecutorUniformScopesDistribution: public TTestingExecutor {
    private:
        virtual TString GetConveyorConfig() override {
            return Sprintf(R"(
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "insert"
                        Weight: 1
                    }
                }
                Categories {
                    Name: "insert"
                }
                )",
                GetThreadsCount());
        }
        virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() override {
            return { std::make_shared<TSimpleRequest>("1", ESpecialTaskCategory::Insert, "1", 1),
                std::make_shared<TSimpleRequest>("2", ESpecialTaskCategory::Insert, "2", 2),
                std::make_shared<TSimpleRequest>("3", ESpecialTaskCategory::Insert, "3", 3) };
        }

    public:
    };
    Y_UNIT_TEST(TestUniformScopesDistribution) {
        TTestingExecutorUniformScopesDistribution().Execute();
    }

    class TTestingExecutorUniformDistribution: public TTestingExecutor {
    private:
        virtual ui32 GetTasksCount() const override {
            return 1000000;
        }
        virtual double GetThreadsCount() const override {
            return 16.4;
        }
        virtual TString GetConveyorConfig() override {
            return Sprintf(R"(
                WorkerPools {
                    WorkersCount: %f
                    Links {
                        Category: "insert"
                        Weight: 0.1
                    }
                    Links {
                        Category: "scan"
                        Weight: 0.1
                    }
                    Links {
                        Category: "normalizer"
                        Weight: 0.1
                    }
                }
                Categories {
                    Name: "insert"
                }
                Categories {
                    Name: "normalizer"
                }
                Categories {
                    Name: "scan"
                }
                )",
                GetThreadsCount());
        }
        virtual std::vector<std::shared_ptr<IRequestProcessor>> GetRequests() override {
            return { std::make_shared<TSimpleRequest>("I_1_1", ESpecialTaskCategory::Insert, "1", 1),
                std::make_shared<TSimpleRequest>("I_2_1", ESpecialTaskCategory::Insert, "2", 2),
                std::make_shared<TSimpleRequest>("I_3_1", ESpecialTaskCategory::Insert, "3", 3),
                std::make_shared<TSimpleRequest>("S_1_1", ESpecialTaskCategory::Scan, "1", 4),
                std::make_shared<TSimpleRequest>("S_2_1", ESpecialTaskCategory::Scan, "2", 5),
                std::make_shared<TSimpleRequest>("S_3_1", ESpecialTaskCategory::Scan, "3", 6),
                std::make_shared<TSimpleRequest>("N_1_1", ESpecialTaskCategory::Normalizer, "1", 7),
                std::make_shared<TSimpleRequest>("N_2_1", ESpecialTaskCategory::Normalizer, "2", 8),
                std::make_shared<TSimpleRequest>("N_3_1", ESpecialTaskCategory::Normalizer, "3", 9),
                std::make_shared<TSimpleRequest>("I_1_2", ESpecialTaskCategory::Insert, "1", 21),
                std::make_shared<TSimpleRequest>("I_2_2", ESpecialTaskCategory::Insert, "2", 22),
                std::make_shared<TSimpleRequest>("I_3_2", ESpecialTaskCategory::Insert, "3", 23),
                std::make_shared<TSimpleRequest>("S_1_2", ESpecialTaskCategory::Scan, "1", 24),
                std::make_shared<TSimpleRequest>("S_2_2", ESpecialTaskCategory::Scan, "2", 25),
                std::make_shared<TSimpleRequest>("S_3_2", ESpecialTaskCategory::Scan, "3", 26),
                std::make_shared<TSimpleRequest>("N_1_2", ESpecialTaskCategory::Normalizer, "1", 27),
                std::make_shared<TSimpleRequest>("N_2_2", ESpecialTaskCategory::Normalizer, "2", 28),
                std::make_shared<TSimpleRequest>("N_3_2", ESpecialTaskCategory::Normalizer, "3", 29) };
        }

    public:
    };
    Y_UNIT_TEST(TestUniformDistribution) {
        TTestingExecutorUniformDistribution().Execute();
    }

    Y_UNIT_TEST(ParseActorSystemPool) {
        {
            auto parsed = NConfig::ParseActorSystemPool("User");
            UNIT_ASSERT(parsed.IsSuccess());
            UNIT_ASSERT(*parsed == EActorSystemPool::User);
        }
        {
            auto parsed = NConfig::ParseActorSystemPool("Batch");
            UNIT_ASSERT(parsed.IsSuccess());
            UNIT_ASSERT(*parsed == EActorSystemPool::Batch);
        }
        UNIT_ASSERT(NConfig::ParseActorSystemPool("user").IsFail());
        UNIT_ASSERT(NConfig::ParseActorSystemPool("USER").IsFail());
        UNIT_ASSERT(NConfig::ParseActorSystemPool("batch").IsFail());
        UNIT_ASSERT(NConfig::ParseActorSystemPool("System").IsFail());
        UNIT_ASSERT(NConfig::ParseActorSystemPool("").IsFail());
    }

    Y_UNIT_TEST(HeavyLimitsParseErrors) {
        auto expectFail = [](const TString& textProto) {
            NKikimrConfig::TCompositeConveyorConfig protoConfig;
            AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));
            UNIT_ASSERT(NConfig::TConfig::BuildFromProto(protoConfig).IsFail());
        };
        expectFail(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 25000000 ThreadLimit: 0 }
            }
            Categories { Name: "scan" }
        )");
        expectFail(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000000 ThreadLimit: 4 }
                HeavyLimits { CpuLimitUs: 25000000 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
        expectFail(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 25000000 ThreadLimit: 8 }
                HeavyLimits { CpuLimitUs: 50000000 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
        expectFail(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 0 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
        expectFail(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 4
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
    }

    Y_UNIT_TEST(OverlayYamlOnDefaultsByName) {
        NKikimrConfig::TCompositeConveyorConfig defaults;
        {
            auto* pool = defaults.AddWorkerPools();
            pool->SetName("scan");
            pool->SetDefaultFractionOfThreadsCount(0.4);
            auto* link = pool->AddLinks();
            link->SetCategory("scan");
            link->SetWeight(1);
            auto* cat = defaults.AddCategories();
            cat->SetName("scan");
        }
        {
            auto* pool = defaults.AddWorkerPools();
            pool->SetName("compaction");
            pool->SetDefaultFractionOfThreadsCount(0.33);
            auto* link = pool->AddLinks();
            link->SetCategory("compaction");
            link->SetWeight(1);
            auto* cat = defaults.AddCategories();
            cat->SetName("compaction");
        }

        NKikimrConfig::TCompositeConveyorConfig yaml;
        {
            auto* pool = yaml.AddWorkerPools();
            pool->SetName("scan");
            auto* limit = pool->AddHeavyLimits();
            limit->SetCpuLimitUs(25000000);
            limit->SetThreadLimit(8);
            auto* limit2 = pool->AddHeavyLimits();
            limit2->SetCpuLimitUs(50000000);
            limit2->SetThreadLimit(4);
        }

        auto overlaid = NConfig::TConfig::OverlayYamlOnDefaults(defaults, yaml).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(0).GetName(), "scan");
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(0).GetHeavyLimits().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(0).GetLinks().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(1).GetName(), "compaction");
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(1).GetHeavyLimits().size(), 0);

        NKikimrConfig::TCompositeConveyorConfig unknown;
        auto* unknownPool = unknown.AddWorkerPools();
        unknownPool->SetName("Scan");
        unknownPool->AddHeavyLimits()->SetCpuLimitUs(1);
        unknownPool->MutableHeavyLimits(0)->SetThreadLimit(1);
        UNIT_ASSERT(NConfig::TConfig::OverlayYamlOnDefaults(defaults, unknown).IsFail());
        UNIT_ASSERT_STRING_CONTAINS(
            NConfig::TConfig::OverlayYamlOnDefaults(defaults, unknown).GetErrorMessage(), "expected one of");
    }

    Y_UNIT_TEST(OverlayPartialCategoriesKeepsDefaults) {
        NKikimrConfig::TCompositeConveyorConfig defaults;
        {
            auto* scan = defaults.AddCategories();
            scan->SetName("scan");
            scan->SetQueueSizeLimit(10);
            auto* compaction = defaults.AddCategories();
            compaction->SetName("compaction");
            compaction->SetQueueSizeLimit(20);
        }
        {
            auto* pool = defaults.AddWorkerPools();
            pool->SetName("scan");
            auto* link = pool->AddLinks();
            link->SetCategory("scan");
            link->SetWeight(1);
        }

        NKikimrConfig::TCompositeConveyorConfig yaml;
        {
            auto* scan = yaml.AddCategories();
            scan->SetName("scan");
            scan->SetQueueSizeLimit(99);
            auto* pool = yaml.AddWorkerPools();
            pool->SetName("scan");
            auto* limit = pool->AddHeavyLimits();
            limit->SetCpuLimitUs(25000000);
            limit->SetThreadLimit(8);
        }

        auto overlaid = NConfig::TConfig::OverlayYamlOnDefaults(defaults, yaml).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetCategories().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetCategories(0).GetName(), "scan");
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetCategories(0).GetQueueSizeLimit(), 99);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetCategories(1).GetName(), "compaction");
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetCategories(1).GetQueueSizeLimit(), 20);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(0).GetHeavyLimits().size(), 1);
    }

    Y_UNIT_TEST(OverlayWithLinksKeepsDefaultMaxBatchSize) {
        NKikimrConfig::TCompositeConveyorConfig defaults;
        {
            auto* pool = defaults.AddWorkerPools();
            pool->SetName("scan");
            pool->SetDefaultFractionOfThreadsCount(0.4);
            auto* link = pool->AddLinks();
            link->SetCategory("scan");
            link->SetWeight(1);
        }
        {
            auto* pool = defaults.AddWorkerPools();
            pool->SetName("compaction");
            pool->SetMaxBatchSize(1);
            pool->SetDefaultFractionOfThreadsCount(0.33);
            auto* link = pool->AddLinks();
            link->SetCategory("compaction");
            link->SetWeight(1);
        }

        NKikimrConfig::TCompositeConveyorConfig yaml;
        {
            auto* pool = yaml.AddWorkerPools();
            pool->SetName("scan");
            auto* limit = pool->AddHeavyLimits();
            limit->SetCpuLimitUs(25000000);
            limit->SetThreadLimit(8);
        }
        {
            auto* pool = yaml.AddWorkerPools();
            pool->SetName("compaction");
            auto* link = pool->AddLinks();
            link->SetCategory("compaction");
            link->SetWeight(1);
            pool->SetWorkersCount(4);
        }

        auto overlaid = NConfig::TConfig::OverlayYamlOnDefaults(defaults, yaml).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools().size(), 2);
        const auto& compaction = overlaid.GetWorkerPools(1);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetName(), "compaction");
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetMaxBatchSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetWorkersCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(compaction.GetDefaultFractionOfThreadsCount(), 0.33);
        UNIT_ASSERT_VALUES_EQUAL(overlaid.GetWorkerPools(0).GetHeavyLimits().size(), 1);
    }

    Y_UNIT_TEST(NoHeavyLimitsUsesWorkersBeyondLimit) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
            }
            Categories { Name: "scan" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));
        const ui64 processId = 1;
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000, 1), ESpecialTaskCategory::Scan, "s", processId));

        TAtomicCounter recordedDone;
        std::array<TAtomicCounter, 16> perWorker;
        const ui32 recordedTasks = 32;
        for (ui32 i = 0; i < recordedTasks; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                ESpecialTaskCategory::Scan, processId));
        }
        WaitCounter(recordedDone, recordedTasks);

        ui32 unrestrictedCount = 0;
        for (ui32 i = 8; i < perWorker.size(); ++i) {
            unrestrictedCount += perWorker[i].Val();
        }
        UNIT_ASSERT_C(unrestrictedCount > 0, "without heavy_limits a scan must use workers beyond index 8");

        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, processId));
        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(DefaultScanProcessIsNotPessimized) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));

        WaitWarmupAccounted(actorSystem, actorId, ESpecialTaskCategory::Scan, 0, 4, TDuration::MilliSeconds(20));

        TAtomicCounter recordedDone;
        std::array<TAtomicCounter, 16> perWorker;
        const ui32 recordedTasks = 32;
        for (ui32 i = 0; i < recordedTasks; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                ESpecialTaskCategory::Scan, 0));
        }
        WaitCounter(recordedDone, recordedTasks);

        ui32 unrestrictedCount = 0;
        for (ui32 i = 8; i < perWorker.size(); ++i) {
            unrestrictedCount += perWorker[i].Val();
        }
        UNIT_ASSERT_C(unrestrictedCount > 0, "shared process 0 must not be pinned by heavy_limits");

        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(PessimizedProcessUsesOnlyFirstWorkers) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
            }
            Categories { Name: "scan" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));
        const ui64 processId = 1;
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000, 1), ESpecialTaskCategory::Scan, "s", processId));

        WaitWarmupAccounted(actorSystem, actorId, ESpecialTaskCategory::Scan, processId, 4, TDuration::MilliSeconds(20));

        TAtomicCounter recordedDone;
        std::array<TAtomicCounter, 16> perWorker;
        const ui32 recordedTasks = 32;
        for (ui32 i = 0; i < recordedTasks; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                ESpecialTaskCategory::Scan, processId));
        }
        WaitCounter(recordedDone, recordedTasks);

        ui32 restrictedCount = 0;
        ui32 unrestrictedCount = 0;
        for (ui32 i = 0; i < perWorker.size(); ++i) {
            if (i < 8) {
                restrictedCount += perWorker[i].Val();
            } else {
                unrestrictedCount += perWorker[i].Val();
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(restrictedCount, recordedTasks);
        UNIT_ASSERT_VALUES_EQUAL(unrestrictedCount, 0);

        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, processId));
        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(TwoTierHeavyLimits) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
                HeavyLimits { CpuLimitUs: 150000 ThreadLimit: 4 }
            }
            Categories { Name: "scan" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));

        const auto runRecorded = [&](const ui64 processId, const ui32 warmupTasks, const ui32 warmupMs) {
            actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000, 1), ESpecialTaskCategory::Scan, "s", processId));
            WaitWarmupAccounted(actorSystem, actorId, ESpecialTaskCategory::Scan, processId, warmupTasks, TDuration::MilliSeconds(warmupMs));
            TAtomicCounter recordedDone;
            std::array<TAtomicCounter, 16> perWorker;
            const ui32 recordedTasks = 32;
            for (ui32 i = 0; i < recordedTasks; ++i) {
                actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                    std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                    ESpecialTaskCategory::Scan, processId));
            }
            WaitCounter(recordedDone, recordedTasks);
            actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, processId));
            return perWorker;
        };

        {
            const auto perWorker = runRecorded(1, 4, 20);
            ui32 unrestrictedCount = 0;
            for (ui32 i = 8; i < perWorker.size(); ++i) {
                unrestrictedCount += perWorker[i].Val();
            }
            UNIT_ASSERT_VALUES_EQUAL(unrestrictedCount, 0);
        }
        {
            const auto perWorker = runRecorded(2, 10, 20);
            ui32 midAndAbove = 0;
            for (ui32 i = 4; i < perWorker.size(); ++i) {
                midAndAbove += perWorker[i].Val();
            }
            UNIT_ASSERT_VALUES_EQUAL(midAndAbove, 0);
        }

        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(InsertProcessIsNotPessimized) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
            }
            WorkerPools {
                Name: "insert"
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "insert" Weight: 1 }
            }
            Categories { Name: "scan" }
            Categories { Name: "insert" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));

        {
            TAtomicCounter warmupDone;
            const ui32 warmupTasks = 4;
            for (ui32 i = 0; i < warmupTasks; ++i) {
                actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                    std::make_shared<TSleepTask>(TDuration::MilliSeconds(20), warmupDone), ESpecialTaskCategory::Insert, 0));
            }
            WaitCounter(warmupDone, warmupTasks);
        }

        TAtomicCounter recordedDone;
        std::array<TAtomicCounter, 16> perWorker;
        const ui32 recordedTasks = 32;
        for (ui32 i = 0; i < recordedTasks; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                ESpecialTaskCategory::Insert, 0));
        }
        WaitCounter(recordedDone, recordedTasks);

        ui32 unrestrictedCount = 0;
        for (ui32 i = 8; i < perWorker.size(); ++i) {
            unrestrictedCount += perWorker[i].Val();
        }
        UNIT_ASSERT_C(unrestrictedCount > 0, "insert must keep using workers beyond the scan heavy_limits");

        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(CompactionProcessIsNotPessimized) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);
        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const auto config = ParseConveyorProto(R"(
            WorkerPools {
                Name: "scan"
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "scan" Weight: 1 }
                HeavyLimits { CpuLimitUs: 50000 ThreadLimit: 8 }
            }
            WorkerPools {
                Name: "compaction"
                WorkersCount: 16
                MaxBatchSize: 1
                Links { Category: "compaction" Weight: 1 }
            }
            Categories { Name: "scan" }
            Categories { Name: "compaction" }
        )");
        const auto actorId = actorSystem.Register(CreateService(config, counters));

        {
            TAtomicCounter warmupDone;
            const ui32 warmupTasks = 4;
            for (ui32 i = 0; i < warmupTasks; ++i) {
                actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                    std::make_shared<TSleepTask>(TDuration::MilliSeconds(20), warmupDone), ESpecialTaskCategory::Compaction, 0));
            }
            WaitCounter(warmupDone, warmupTasks);
        }

        TAtomicCounter recordedDone;
        std::array<TAtomicCounter, 16> perWorker;
        const ui32 recordedTasks = 32;
        for (ui32 i = 0; i < recordedTasks; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(
                std::make_shared<TWorkerRecordingTask>(TDuration::MilliSeconds(2), recordedDone, &perWorker),
                ESpecialTaskCategory::Compaction, 0));
        }
        WaitCounter(recordedDone, recordedTasks);

        ui32 unrestrictedCount = 0;
        for (ui32 i = 8; i < perWorker.size(); ++i) {
            unrestrictedCount += perWorker[i].Val();
        }
        UNIT_ASSERT_C(unrestrictedCount > 0, "compaction must keep using workers beyond the scan heavy_limits");

        actorSystem.Stop();
        actorSystem.Cleanup();
    }
}
