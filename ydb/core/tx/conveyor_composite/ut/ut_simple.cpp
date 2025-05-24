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
        Sleep(ExecutionTime);
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
    Y_UNIT_TEST(IndexWriteOverload) {
        const ui64 threadsCount = 64;
        THolder<NActors::TActorSystemSetup> actorSystemSetup = NKikimr::BuildActorSystemSetup(threadsCount, 1);
        NActors::TActorSystem actorSystem(actorSystemSetup);

        actorSystem.Start();
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const std::string text_proto = R"(
            WorkerPools {
                WorkersCount: 9.5
                Links {
                    Category: "insert"
                    Weight: 0.1
                }
                Links {
                    Category: "scan"
                    Weight: 0.01
                }
            }
            Categories {
                Name: "insert"
            }
            Categories {
                Name: "scan"
            }
        )";
        NKikimrConfig::TCompositeConveyorConfig protoConfig;
        AFL_VERIFY(google::protobuf::TextFormat::ParseFromString(text_proto, &protoConfig));

        NConfig::TConfig config;
        config.DeserializeFromProto(protoConfig, threadsCount).Validate();
        const auto actorId = actorSystem.Register(TCompServiceOperator::CreateService(config, counters));

        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvRegisterProcess(TCPULimitsConfig(1000), ESpecialTaskCategory::Scan, "1", 1));

        TAtomicCounter CounterInsert;
        TAtomicCounter CounterScan;
        const i64 tasksCount = 1000000;
        const TMonotonic startGlobal = TMonotonic::Now();
        for (i32 i = 0; i < tasksCount; ++i) {
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), CounterInsert),
                                          ESpecialTaskCategory::Insert, "1", 1));
            actorSystem.Send(actorId, new TEvExecution::TEvNewTask(std::make_shared<TSleepTask>(TDuration::MicroSeconds(40), CounterScan),
                                          ESpecialTaskCategory::Scan, "1", 1));
        }

        while (CounterInsert.Val() < tasksCount || CounterScan.Val() < tasksCount) {
            Cerr << "I:" << CounterInsert.Val() << ";S:" << CounterScan.Val() << ";" << Endl;
            Sleep(TDuration::Seconds(1));
        }
        Cerr << "I:" << CounterInsert.Val() << ";S:" << CounterScan.Val() << ";"
             << ((TMonotonic::Now() - startGlobal) / (2.0 * tasksCount / threadsCount)).MicroSeconds() << Endl;


        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Insert, "1", 1));
        actorSystem.Send(actorId, new TEvExecution::TEvUnregisterProcess(ESpecialTaskCategory::Scan, "1", 1));
        Sleep(TDuration::Seconds(5));

        actorSystem.Stop();
        actorSystem.Cleanup();
    }
}
