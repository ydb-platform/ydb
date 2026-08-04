#include <ydb/core/base/appdata.h>
#include <ydb/core/tablet/resource_broker_impl.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash.h>
#include <util/stream/str.h>
#include <util/system/yassert.h>

namespace {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NResourceBroker;

constexpr size_t MaxOperations = 192;

class TNullActor final : public TActorBootstrapped<TNullActor> {
public:
    void Bootstrap(const TActorContext&) {
        Become(&TNullActor::StateFunc);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TSystem::Poison, PassAway)
        default:
            break;
        }
    }
};

THolder<TActorSystemSetup> MakeSetup(const TActorId& sender1, const TActorId& sender2) {
    auto setup = MakeHolder<TActorSystemSetup>();
    setup->NodeId = 1;
    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 20));
    setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(128, 10)));
    setup->LocalServices.emplace_back(sender1, TActorSetupCmd(new TNullActor, TMailboxType::Simple, 0));
    setup->LocalServices.emplace_back(sender2, TActorSetupCmd(new TNullActor, TMailboxType::Simple, 0));
    return setup;
}

NKikimrResourceBroker::TResourceBrokerConfig MakeFuzzConfig(FuzzedDataProvider& fdp) {
    NKikimrResourceBroker::TResourceBrokerConfig config;

    auto addQueue = [&](const TString& name) {
        auto* queue = config.AddQueues();
        queue->SetName(name);
        queue->SetWeight(fdp.ConsumeIntegralInRange<ui32>(1, 32));
        queue->MutableLimit()->AddResource(fdp.ConsumeIntegralInRange<ui64>(1, 1024));
        queue->MutableLimit()->AddResource(fdp.ConsumeIntegralInRange<ui64>(1, 1024));
    };

    addQueue("queue_default");
    addQueue("queue_fast");
    addQueue("queue_slow");

    auto addTask = [&](const TString& name, const TString& queue) {
        auto* task = config.AddTasks();
        task->SetName(name);
        task->SetQueueName(queue);
        task->SetDefaultDuration(TDuration::MilliSeconds(fdp.ConsumeIntegralInRange<ui32>(1, 30000)).GetValue());
    };

    addTask("unknown", "queue_default");
    addTask("fast", "queue_fast");
    addTask("slow", "queue_slow");

    config.MutableResourceLimit()->AddResource(fdp.ConsumeIntegralInRange<ui64>(1, 2048));
    config.MutableResourceLimit()->AddResource(fdp.ConsumeIntegralInRange<ui64>(1, 2048));
    return config;
}

TString PickType(FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<unsigned>(0, 3)) {
        case 0:
            return "unknown";
        case 1:
            return "fast";
        case 2:
            return "slow";
        default:
            return fdp.ConsumeBool() ? "missing" : "";
    }
}

TResourceValues PickResources(FuzzedDataProvider& fdp) {
    TResourceValues values{};
    values[0] = fdp.ConsumeIntegralInRange<ui64>(0, 1024);
    values[1] = fdp.ConsumeIntegralInRange<ui64>(0, 1024);
    return values;
}

ui64 PickKnownOrRandom(const THashSet<ui64>& known, FuzzedDataProvider& fdp) {
    if (!known.empty() && fdp.ConsumeBool()) {
        auto it = known.begin();
        std::advance(it, fdp.ConsumeIntegralInRange<size_t>(0, known.size() - 1));
        return *it;
    }
    return fdp.ConsumeIntegralInRange<ui64>(1, 64);
}

void RunResourceBrokerFuzz(const ui8* data, size_t size) {
    if (size > 4096) {
        size = 4096;
    }

    FuzzedDataProvider fdp(data, size);
    const TActorId sender1{1, "sender1"};
    const TActorId sender2{1, "sender2"};
    auto setup = MakeSetup(sender1, sender2);
    TAppData appData(0, 1, 0, 1, {}, nullptr, nullptr, nullptr, nullptr);
    TActorSystem actorSystem(setup, &appData);
    actorSystem.Start();

    auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    TResourceBroker broker(MakeFuzzConfig(fdp), counters, &actorSystem);
    THashSet<ui64> known;

    for (size_t step = 0; step < MaxOperations && fdp.remaining_bytes(); ++step) {
        const TActorId sender = fdp.ConsumeBool() ? sender1 : sender2;

        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 9)) {
            case 0:
            case 1:
            case 2: {
                const ui64 taskId = fdp.ConsumeIntegralInRange<ui64>(1, 64);
                TEvResourceBroker::TEvSubmitTask ev(
                    taskId,
                    TStringBuilder() << "task-" << taskId,
                    PickResources(fdp),
                    PickType(fdp),
                    fdp.ConsumeIntegralInRange<ui64>(0, 16),
                    nullptr);
                auto error = broker.SubmitTask(ev, sender);
                if (!error) {
                    known.insert(taskId);
                }
                break;
            }

            case 3: {
                const ui64 taskId = PickKnownOrRandom(known, fdp);
                TEvResourceBroker::TEvUpdateTask ev(
                    taskId,
                    PickResources(fdp),
                    PickType(fdp),
                    fdp.ConsumeIntegralInRange<ui64>(0, 16),
                    fdp.ConsumeBool());
                broker.UpdateTask(ev, sender);
                break;
            }

            case 4: {
                const ui64 taskId = PickKnownOrRandom(known, fdp);
                TEvResourceBroker::TEvRemoveTask ev(taskId, fdp.ConsumeBool());
                auto error = broker.RemoveTask(ev, sender);
                if (!error) {
                    known.erase(taskId);
                }
                break;
            }

            case 5: {
                const ui64 taskId = PickKnownOrRandom(known, fdp);
                TEvResourceBroker::TEvFinishTask ev(taskId, fdp.ConsumeBool());
                auto error = broker.FinishTask(ev, sender);
                if (!error) {
                    known.erase(taskId);
                }
                break;
            }

            case 6: {
                const ui64 taskId = fdp.ConsumeIntegralInRange<ui64>(65, 128);
                TEvResourceBroker::TEvSubmitTask ev(
                    taskId,
                    TStringBuilder() << "instant-" << taskId,
                    PickResources(fdp),
                    PickType(fdp),
                    fdp.ConsumeIntegralInRange<ui64>(0, 16),
                    nullptr);
                if (broker.SubmitTaskInstant(ev, sender)) {
                    known.insert(taskId);
                }
                break;
            }

            case 7: {
                const ui64 taskId = PickKnownOrRandom(known, fdp);
                TEvResourceBroker::TEvFinishTask ev(taskId, fdp.ConsumeBool());
                if (broker.FinishTaskInstant(ev, sender)) {
                    known.erase(taskId);
                }
                break;
            }

            case 8: {
                if (known.size() >= 2) {
                    const ui64 first = PickKnownOrRandom(known, fdp);
                    const ui64 second = PickKnownOrRandom(known, fdp);
                    if (first != second && broker.MergeTasksInstant(first, second, sender)) {
                        known.erase(second);
                    }
                }
                break;
            }

            case 9: {
                const ui64 taskId = PickKnownOrRandom(known, fdp);
                broker.ReduceTaskResourcesInstant(taskId, PickResources(fdp), sender);
                break;
            }
        }

        if (fdp.ConsumeIntegralInRange<unsigned>(0, 15) == 0) {
            TStringStream state;
            broker.OutputState(state);
        }
    }

    TStringStream state;
    broker.OutputState(state);
    actorSystem.Stop();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    RunResourceBrokerFuzz(data, size);
    return 0;
}
