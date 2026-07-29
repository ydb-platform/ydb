#include "test_runtime.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {
namespace {

    class TMailboxProcessingFinishedActor
        : public TActorBootstrapped<TMailboxProcessingFinishedActor>
    {
    public:
        explicit TMailboxProcessingFinishedActor(TActorId edgeActor)
            : EdgeActor(edgeActor)
        {}

        void Bootstrap() {
            SetSystemFlag(ESystemFlag::MailboxProcessingFinished);
            Become(&TThis::StateWork);
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvMailboxProcessingFinished, Handle);
            }
        }

        void Handle(TEvents::TEvMailboxProcessingFinished::TPtr& ev) {
            UNIT_ASSERT(
                ev->Get()->Reason ==
                TEvents::TEvMailboxProcessingFinished::EReason::QueueEmpty);
            Send(EdgeActor, new TEvents::TEvWakeup(ev->Get()->ExecutedEvents));
            PassAway();
        }

    private:
        const TActorId EdgeActor;
    };

}

Y_UNIT_TEST_SUITE(TTestActorRuntimeMailboxProcessingFinished) {

    Y_UNIT_TEST(DeliversNotification) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();

        const TActorId edgeActor = runtime.AllocateEdgeActor();
        runtime.Register(new TMailboxProcessingFinishedActor(edgeActor));

        TAutoPtr<IEventHandle> handle;
        const auto* result = runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        UNIT_ASSERT_VALUES_EQUAL(result->Tag, 1);
    }

}
}
