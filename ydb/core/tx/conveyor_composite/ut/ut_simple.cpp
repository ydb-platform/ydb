#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/signals/object_counter.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

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

    TSleepTask(const TDuration d, TAtomicCounter& c)
        : ExecutionTime(d)
        , Counter(&c) {
    }
};

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
        const auto actorId = actorSystem.Register(TServiceOperator::CreateService(config, counters));

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
        auto actual = []() {
            return NKikimr::NColumnShard::TMonitoringObjectsCounter<TProcessScope>::GetCounter().Val();
        };
        int timeout = 60;

        for (int i = 0; i < timeout && actual() != expected; ++i) {
            Sleep(TDuration::Seconds(1));
        }
        AFL_VERIFY(actual() == expected)("count", actual());

        actorSystem.Stop();
        actorSystem.Cleanup();
    };
};

Y_UNIT_TEST_SUITE(CompositeConveyorTests) {
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
}
