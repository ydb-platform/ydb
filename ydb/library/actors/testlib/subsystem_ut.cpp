#include "test_runtime.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    class TTimerService : public TActorBootstrapped<TTimerService> {
        TActorId Edge;
    public:
        void Bootstrap() {
            Become(&TThis::StateRequest);
        }
        STFUNC(StateRequest) {
            if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                Edge = ev->Sender;
                Become(&TThis::StateWork);
                Schedule(TDuration::MilliSeconds(1), new TEvents::TEvWakeup);
            }
        }
        STFUNC(StateWork) {
            if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                Send(Edge, new TEvents::TEvWakeup);
                PassAway();
            }
        }
    };

    class ITestSubsystem : public ISubSystem {
    public:
        virtual ui32 GetNodeIndex() const = 0;
    };

    class TTestSubsystem final : public ITestSubsystem {
        const ui32 NodeIndex;

    public:
        bool Started = false;

        explicit TTestSubsystem(ui32 nodeIndex)
            : NodeIndex(nodeIndex)
        {}

        ui32 GetNodeIndex() const override {
            return NodeIndex;
        }

        void OnBeforeStart(TActorSystem&) override {
            Started = true;
        }
    };

    void CheckNodeSubsystems(bool realThreads) {
        TTestActorRuntimeBase runtime(2, realThreads);
        ui32 registrations = 0;
        runtime.SetupNodeSubSystems = [&registrations](ui32 nodeIndex, TActorSystemSetup* setup) {
            ++registrations;
            setup->RegisterSubSystem<ITestSubsystem>(std::make_unique<TTestSubsystem>(nodeIndex));
        };
        runtime.Initialize();

        UNIT_ASSERT_VALUES_EQUAL(registrations, 2);
        for (ui32 nodeIndex = 0; nodeIndex < 2; ++nodeIndex) {
            auto* subsystem = runtime.GetActorSystem(nodeIndex)->GetSubSystem<ITestSubsystem>();
            UNIT_ASSERT(subsystem);
            UNIT_ASSERT_VALUES_EQUAL(subsystem->GetNodeIndex(), nodeIndex);
            UNIT_ASSERT(dynamic_cast<TTestSubsystem*>(subsystem)->Started);
        }
        UNIT_ASSERT(runtime.GetActorSystem(0)->GetSubSystem<ITestSubsystem>() !=
            runtime.GetActorSystem(1)->GetSubSystem<ITestSubsystem>());
    }
}

Y_UNIT_TEST_SUITE(TestRuntimeSubsystems) {
    Y_UNIT_TEST(SimulatedRuntime) {
        CheckNodeSubsystems(false);
    }

    Y_UNIT_TEST(RealThreadsRuntime) {
        CheckNodeSubsystems(true);
    }
    Y_UNIT_TEST(SetupServicesSupportLookupAndTimers) {
        TTestActorRuntimeBase runtime;
        const TActorId service(0, "testtimer");
        auto* actor = new TTimerService;
        runtime.SetupNodeSubSystems = [service, actor](ui32, TActorSystemSetup* setup) {
            setup->LocalServices.emplace_back(service,
                TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0));
        };
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event, TDuration, TInstant&) {
            return !runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite());
        });
        runtime.EnableScheduleForActor(service);
        runtime.Initialize();
        UNIT_ASSERT(runtime.FindActor(service, ui32{0}) == actor);
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Send(service, edge, new TEvents::TEvWakeup, 0, true);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(edge, TDuration::Seconds(1)));
    }

}
