#include "test_runtime.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
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
}
