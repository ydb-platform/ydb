#include "test_runtime.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {
namespace {

    class TMailboxProcessingFinishedActor
        : public TActorBootstrapped<TMailboxProcessingFinishedActor>
    {
    public:
        TMailboxProcessingFinishedActor(
                TActorId edgeActor,
                TEvents::TEvMailboxProcessingFinished::EReason expectedReason,
                ui32 messages = 0)
            : EdgeActor(edgeActor)
            , ExpectedReason(expectedReason)
            , Messages(messages)
        {}

        void Bootstrap() {
            SetSystemFlag(ESystemFlag::MailboxProcessingFinished);
            Become(&TThis::StateWork);
            for (ui32 i = 0; i < Messages; ++i) {
                Send(SelfId(), new TEvents::TEvWakeup(i));
            }
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvMailboxProcessingFinished, Handle);
                cFunc(TEvents::TSystem::Wakeup, Ignore);
            }
        }

        void Handle(TEvents::TEvMailboxProcessingFinished::TPtr& ev) {
            UNIT_ASSERT(ev->Get()->Reason == ExpectedReason);
            Send(EdgeActor, new TEvents::TEvWakeup(ev->Get()->ExecutedEvents));
            PassAway();
        }

        void Ignore() {
        }

    private:
        const TActorId EdgeActor;
        const TEvents::TEvMailboxProcessingFinished::EReason ExpectedReason;
        const ui32 Messages;
    };

}

Y_UNIT_TEST_SUITE(TTestActorRuntimeMailboxProcessingFinished) {

    Y_UNIT_TEST(DeliversNotification) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(new TMailboxProcessingFinishedActor(
            edgeActor,
            TEvents::TEvMailboxProcessingFinished::EReason::QueueEmpty));

        TAutoPtr<IEventHandle> handle;
        const auto* result = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Tag, 1);
    }

    Y_UNIT_TEST(BatchesAtLeastEightEvents) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(new TMailboxProcessingFinishedActor(
            edgeActor,
            TEvents::TEvMailboxProcessingFinished::EReason::EventCountLimitReached,
            8));

        TAutoPtr<IEventHandle> handle;
        const auto* result = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Tag, 8);
    }

}
}
