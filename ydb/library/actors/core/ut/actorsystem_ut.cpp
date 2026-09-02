#include "actorsystem.h"

#include "actor_bootstrapped.h"
#include "events.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TActorSystemTest) {

    class TTestActor: public TActor<TTestActor> {
    public:
        TTestActor()
            : TActor{&TThis::Main}
        {
        }

        STATEFN(Main) {
            Y_UNUSED(ev);
        }
    };

    THolder<TTestActorRuntimeBase> CreateRuntime() {
        auto runtime = MakeHolder<TTestActorRuntimeBase>();
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->Initialize();
        return runtime;
    }

    Y_UNIT_TEST(LocalService) {
        THolder<TTestActorRuntimeBase> runtime = CreateRuntime();
        auto actorA = runtime->Register(new TTestActor);
        auto actorB = runtime->Register(new TTestActor);

        TActorId myServiceId{0, TStringBuf{"my-service"}};

        auto prevActorId = runtime->RegisterService(myServiceId, actorA);
        UNIT_ASSERT(!prevActorId);
        UNIT_ASSERT_EQUAL(runtime->GetLocalServiceId(myServiceId), actorA);

        prevActorId = runtime->RegisterService(myServiceId, actorB);
        UNIT_ASSERT(prevActorId);
        UNIT_ASSERT_EQUAL(prevActorId, actorA);
        UNIT_ASSERT_EQUAL(runtime->GetLocalServiceId(myServiceId), actorB);
    }

    constexpr ui32 SelfNodeId = 1;
    constexpr ui32 NodeWithoutProxy = 2;

    class TUndeliveredProbe: public TActorBootstrapped<TUndeliveredProbe> {
    public:
        TUndeliveredProbe(const TActorId& target, ui32 flags, TActorId& undeliveredSender, TManualEvent& done)
            : Target(target)
            , Flags(flags)
            , UndeliveredSender(undeliveredSender)
            , Done(done)
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            Send(Target, new TEvents::TEvPing, Flags);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvents::TEvUndelivered, Handle);
            }
        }

        void Handle(TEvents::TEvUndelivered::TPtr& ev) {
            UndeliveredSender = ev->Sender;
            Done.Signal();
        }

    private:
        const TActorId Target;
        const ui32 Flags;
        TActorId& UndeliveredSender;
        TManualEvent& Done;
    };

    TActorId GetUndeliveredSender(ui32 flags) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = SelfNodeId;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "basic");
        setup->Scheduler = CreateSchedulerThread(TSchedulerConfig());
        setup->Interconnect.ProxyActors.resize(NodeWithoutProxy + 1);

        TActorSystem actorSystem(setup);
        actorSystem.Start();

        TActorId undeliveredSender;
        TManualEvent done;
        actorSystem.Register(new TUndeliveredProbe(
            TActorId(NodeWithoutProxy, 0, 12345, 0), flags, undeliveredSender, done));
        UNIT_ASSERT_C(done.WaitT(TDuration::Seconds(30)), "TEvUndelivered was never received");

        actorSystem.Stop();
        return undeliveredSender;
    }

    Y_UNIT_TEST(UndeliveredKeepsRecipientNodeId) {
        const auto sender = GetUndeliveredSender(IEventHandle::FlagTrackDelivery);
        UNIT_ASSERT_VALUES_EQUAL_C(sender.NodeId(), NodeWithoutProxy,
            "TEvUndelivered must be attributed to the node the event was addressed to, got sender " << sender);
    }

    Y_UNIT_TEST(UndeliveredKeepsRecipientNodeIdWhenSubscribedOnSession) {
        const auto sender = GetUndeliveredSender(
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        UNIT_ASSERT_VALUES_EQUAL_C(sender.NodeId(), NodeWithoutProxy,
            "TEvUndelivered must be attributed to the node the event was addressed to, got sender " << sender);
    }
}
