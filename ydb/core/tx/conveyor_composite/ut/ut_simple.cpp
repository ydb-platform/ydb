#include <ydb/core/tx/conveyor/usage/abstract.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

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

Y_UNIT_TEST_SUITE(TColumnEngineTestLogs) {
    Y_UNIT_TEST(Test10xDistribution) {
        const ui64 threadsCount = 64;
        const double workersCountDouble = 9.5;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);

        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const std::string textProto = Sprintf(R"(
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
        )", workersCountDouble);
        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));

        NConfig::TConfig config;
        config.DeserializeFromProto(protoConfig, threadsCount).Validate();
        const auto actorId = actorSystem.Register(TCompServiceOperator::CreateService(config, counters));

        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Normalizer, "1", 1));

        TAtomicCounter CounterInsert;
        TAtomicCounter CounterScan;
        TAtomicCounter CounterNormalizer;
        const i64 tasksCount = 1000000;
        const TMonotonic startGlobal = TMonotonic::Now();
        for (i32 i = 0; i < tasksCount; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), CounterInsert),
                                          ESpecialTaskCategory::Insert, "1", 1));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), CounterScan),
                                          ESpecialTaskCategory::Scan, "1", 1));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), CounterNormalizer),
                                          ESpecialTaskCategory::Normalizer, "1", 1));
        }

        TDuration dInsert;
        TDuration dScan;
        TDuration dNormalizer;
        while (CounterInsert.Val() < tasksCount || CounterScan.Val() < tasksCount || CounterNormalizer.Val() < tasksCount) {
            Cerr << "I:" << CounterInsert.Val() << ";S:" << CounterScan.Val() << ";" << CounterNormalizer.Val() << ";" << Endl;
            if (CounterInsert.Val() == tasksCount && !dInsert) {
                dInsert = TMonotonic::Now() - startGlobal;
            }
            if (CounterScan.Val() == tasksCount && !dScan) {
                dScan = TMonotonic::Now() - startGlobal;
            }
            if (CounterNormalizer.Val() == tasksCount && !dNormalizer) {
                dNormalizer = TMonotonic::Now() - startGlobal;
            }
            Sleep(TDuration::Seconds(1));
        }
        if (CounterInsert.Val() == tasksCount && !dInsert) {
            dInsert = TMonotonic::Now() - startGlobal;
        }
        if (CounterScan.Val() == tasksCount && !dScan) {
            dScan = TMonotonic::Now() - startGlobal;
        }
        if (CounterNormalizer.Val() == tasksCount && !dNormalizer) {
            dNormalizer = TMonotonic::Now() - startGlobal;
        }
        Cerr << "I:" << CounterInsert.Val() << ";S:" << CounterScan.Val() << ";N:" << CounterNormalizer.Val() << ";"
             << ((TMonotonic::Now() - startGlobal) / (2.0 * tasksCount / workersCountDouble)).MicroSeconds() << ";dScan=" << dScan
             << ";dInsert=" << dInsert << ";dNormalizer=" << dNormalizer << Endl;


        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Normalizer, "1", 1));
        Sleep(TDuration::Seconds(5));

        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(TestUniformDistribution) {
        const ui64 threadsCount = 64;
        const double workersCountDouble = 9.5;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);

        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const std::string textProto = Sprintf(R"(
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
            workersCountDouble);
        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));

        NConfig::TConfig config;
        config.DeserializeFromProto(protoConfig, threadsCount).Validate();
        const auto actorId = actorSystem.Register(TCompServiceOperator::CreateService(config, counters));

        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Insert, "1", 2));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Insert, "1", 3));

        TAtomicCounter Counter1;
        TAtomicCounter Counter2;
        TAtomicCounter Counter3;
        const i64 tasksCount = 1000000;
        const TMonotonic startGlobal = TMonotonic::Now();
        for (i32 i = 0; i < tasksCount; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter1),
                                          ESpecialTaskCategory::Insert, "1", 1));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter2),
                                          ESpecialTaskCategory::Insert, "1", 2));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter3),
                                          ESpecialTaskCategory::Insert, "1", 3));
        }

        TDuration d1;
        TDuration d2;
        TDuration d3;
        while (Counter1.Val() < tasksCount || Counter2.Val() < tasksCount || Counter3.Val() < tasksCount) {
            Cerr << "1:" << Counter1.Val() << ";2:" << Counter2.Val() << ";3:" << Counter3.Val() << ";" << Endl;
            if (Counter1.Val() == tasksCount && !d1) {
                d1 = TMonotonic::Now() - startGlobal;
            }
            if (Counter2.Val() == tasksCount && !d2) {
                d2 = TMonotonic::Now() - startGlobal;
            }
            if (Counter3.Val() == tasksCount && !d3) {
                d3 = TMonotonic::Now() - startGlobal;
            }
            Sleep(TDuration::Seconds(1));
        }
        if (Counter1.Val() == tasksCount && !d1) {
            d1 = TMonotonic::Now() - startGlobal;
        }
        if (Counter2.Val() == tasksCount && !d2) {
            d2 = TMonotonic::Now() - startGlobal;
        }
        if (Counter3.Val() == tasksCount && !d3) {
            d3 = TMonotonic::Now() - startGlobal;
        }
        Cerr << "1:" << Counter1.Val() << ";2:" << Counter2.Val() << ";3:" << Counter3.Val() << ";"
             << ((TMonotonic::Now() - startGlobal) / (2.0 * tasksCount / workersCountDouble)).MicroSeconds() << ";d1=" << d1 << ";d2=" << d2
             << ";d3=" << d3 << Endl;

        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 2));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 3));
        Sleep(TDuration::Seconds(5));

        actorSystem.Stop();
        actorSystem.Cleanup();
    }

    Y_UNIT_TEST(TestScopesDistribution) {
        const ui64 threadsCount = 64;
        const double workersCountDouble = 9.5;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);

        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const std::string textProto = Sprintf(R"(
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
            workersCountDouble);
        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(textProto, &protoConfig));

        NConfig::TConfig config;
        config.DeserializeFromProto(protoConfig, threadsCount).Validate();
        const auto actorId = actorSystem.Register(TCompServiceOperator::CreateService(config, counters));

        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(2, 0.1), ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(2, 0.1), ESpecialTaskCategory::Insert, "2", 2));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(4, 0.1), ESpecialTaskCategory::Insert, "3", 3));

        TAtomicCounter Counter1;
        TAtomicCounter Counter2;
        TAtomicCounter Counter3;
        const i64 tasksCount = 1000000;
        const TMonotonic startGlobal = TMonotonic::Now();
        for (i32 i = 0; i < tasksCount; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter1),
                                          ESpecialTaskCategory::Insert, "1", 1));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter2),
                                          ESpecialTaskCategory::Insert, "2", 2));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), Counter3),
                                          ESpecialTaskCategory::Insert, "3", 3));
        }

        TDuration d1;
        TDuration d2;
        TDuration d3;
        while (Counter1.Val() < tasksCount || Counter2.Val() < tasksCount || Counter3.Val() < tasksCount) {
            Cerr << "1:" << Counter1.Val() << ";2:" << Counter2.Val() << ";3:" << Counter3.Val() << ";" << Endl;
            if (Counter1.Val() == tasksCount && !d1) {
                d1 = TMonotonic::Now() - startGlobal;
            }
            if (Counter2.Val() == tasksCount && !d2) {
                d2 = TMonotonic::Now() - startGlobal;
            }
            if (Counter3.Val() == tasksCount && !d3) {
                d3 = TMonotonic::Now() - startGlobal;
            }
            Sleep(TDuration::Seconds(1));
        }
        if (Counter1.Val() == tasksCount && !d1) {
            d1 = TMonotonic::Now() - startGlobal;
        }
        if (Counter2.Val() == tasksCount && !d2) {
            d2 = TMonotonic::Now() - startGlobal;
        }
        if (Counter3.Val() == tasksCount && !d3) {
            d3 = TMonotonic::Now() - startGlobal;
        }
        Cerr << "1:" << Counter1.Val() << ";2:" << Counter2.Val() << ";3:" << Counter3.Val() << ";"
             << ((TMonotonic::Now() - startGlobal) / (2.0 * tasksCount / workersCountDouble)).MicroSeconds() << ";d1=" << d1 << ";d2=" << d2
             << ";d3=" << d3 << Endl;

        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "2", 2));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "3", 3));
        Sleep(TDuration::Seconds(5));

        actorSystem.Stop();
        actorSystem.Cleanup();
    }
}
