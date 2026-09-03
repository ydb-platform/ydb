#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
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

Y_UNIT_TEST_SUITE(TTestActorSystemMailboxProcessingFinished) {

    Y_UNIT_TEST(DeliversNotification) {
        TTestActorSystem runtime(1);
        runtime.Start();

        const TActorId edgeActor = runtime.AllocateEdgeActor(1);
        runtime.Register(new TMailboxProcessingFinishedActor(edgeActor), 1);

        const std::unique_ptr<IEventHandle> result = runtime.WaitForEdgeActorEvent({edgeActor});
        UNIT_ASSERT(result->GetTypeRewrite() == TEvents::TSystem::Wakeup);
        UNIT_ASSERT_VALUES_EQUAL(result->Get<TEvents::TEvWakeup>()->Tag, 1);

        runtime.Stop();
    }

}
}
