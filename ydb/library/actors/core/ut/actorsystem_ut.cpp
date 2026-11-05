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
    // The barrier shares the observer's mailbox with every synchronous routing
    // result, so duplicate notifications are checked without a timed sleep.
    class TNondeliveryObserver: public TActor<TNondeliveryObserver> {
    public:
        TNondeliveryObserver(TVector<std::unique_ptr<IEventHandle>>& events, TManualEvent& done)
            : TActor(&TThis::StateWork)
            , Events(events)
            , Done(done)
        {}

        STATEFN(StateWork) {
            if (ev->GetTypeRewrite() == TEvents::TEvPing::EventType) {
                Done.Signal();
            } else {
                Events.emplace_back(ev.Release());
            }
        }

    private:
        TVector<std::unique_ptr<IEventHandle>>& Events;
        TManualEvent& Done;
    };

    void CheckNondeliveryForward(bool serialized, bool trackDelivery, bool missingFallback) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = SelfNodeId;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "basic");
        setup->Scheduler = CreateSchedulerThread(TSchedulerConfig());

        TVector<std::unique_ptr<IEventHandle>> events;
        TManualEvent done;
        TActorSystem actorSystem(setup);
        actorSystem.Start();
        const auto observer = actorSystem.Register(new TNondeliveryObserver(events, done));
        // Unknown services fail in GenericSend, before any mailbox enqueue.
        const TActorId missing(0, TStringBuf("missing"));
        const TActorId fallback = missingFallback
            ? TActorId(0, TStringBuf("fallback")) : observer;
        const ui32 flags = IEventHandle::MakeFlags(7,
            IEventHandle::FlagForwardOnNondelivery | IEventHandle::FlagSubscribeOnSession |
            (trackDelivery ? IEventHandle::FlagTrackDelivery : 0));
        constexpr ui64 cookie = 0x123456789abcdef0ULL;
        auto traceId = NWilson::TTraceId::NewTraceId(10, 4095);
        const TString trace = traceId.GetHexFullTraceId();
        auto payload = new TEvents::TEvUndelivered(123, TEvents::TEvUndelivered::Disconnected);
        auto request = std::make_unique<IEventHandle>(missing, observer, payload,
            flags, cookie, &fallback, std::move(traceId));
        TIntrusivePtr<TEventSerializedData> buffer;
        if (serialized) {
            buffer = request->GetChainBuffer();
            request = std::make_unique<IEventHandle>(TEvents::TEvUndelivered::EventType,
                flags, missing, observer, buffer, cookie, &fallback, std::move(request->TraceId));
            UNIT_ASSERT(!request->HasEvent());
            UNIT_ASSERT(request->HasBuffer());
        }

        const bool accepted = actorSystem.Send(std::move(request));
        actorSystem.Send(new IEventHandle(observer, {}, new TEvents::TEvPing));
        const bool completed = done.WaitT(TDuration::Seconds(30));
        actorSystem.Stop();
        UNIT_ASSERT(!accepted);
        UNIT_ASSERT_C(completed, "Nondelivery routing barrier was not received");
        UNIT_ASSERT_VALUES_EQUAL(events.size(), missingFallback && !trackDelivery ? 0 : 1);
        if (events.empty()) {
            return;
        }
        const auto& received = events.front();
        UNIT_ASSERT_VALUES_EQUAL(received->Type, TEvents::TEvUndelivered::EventType);
        UNIT_ASSERT_VALUES_EQUAL(received->Recipient, observer);
        UNIT_ASSERT_VALUES_EQUAL(received->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(received->TraceId.GetHexFullTraceId(), trace);
        if (missingFallback) {
            // Forward was cleared: the second failure reports to the original
            // sender instead of bouncing the payload to the first missing target.
            UNIT_ASSERT_VALUES_EQUAL(received->Sender, fallback);
            UNIT_ASSERT_VALUES_EQUAL(received->Flags, IEventHandle::MakeFlags(7, 0));
            UNIT_ASSERT(!received->GetForwardOnNondeliveryRecipient());
            auto* notification = received->Get<TEvents::TEvUndelivered>();
            UNIT_ASSERT_VALUES_EQUAL(notification->SourceType, TEvents::TEvUndelivered::EventType);
            UNIT_ASSERT_VALUES_EQUAL(notification->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
            UNIT_ASSERT(!notification->Unsure);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(received->Sender, observer);
            UNIT_ASSERT_VALUES_EQUAL(received->GetForwardOnNondeliveryRecipient(), missing);
            UNIT_ASSERT_VALUES_EQUAL(received->Flags,
                flags & ~(IEventHandle::FlagForwardOnNondelivery | IEventHandle::FlagSubscribeOnSession));
            UNIT_ASSERT_VALUES_EQUAL(received->HasEvent(), !serialized);
            if (serialized) {
                UNIT_ASSERT_VALUES_EQUAL(received->GetChainBuffer().Get(), buffer.Get());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(received->GetBase(), payload);
            }
            auto* forwarded = received->Get<TEvents::TEvUndelivered>();
            UNIT_ASSERT_VALUES_EQUAL(forwarded->SourceType, 123);
            UNIT_ASSERT_VALUES_EQUAL(forwarded->Reason, TEvents::TEvUndelivered::Disconnected);
            UNIT_ASSERT(!forwarded->Unsure);
        }
    }

    Y_UNIT_TEST(NondeliveryForwardEvent) {
        CheckNondeliveryForward(false, false, false);
    }

    Y_UNIT_TEST(NondeliveryForwardBuffer) {
        CheckNondeliveryForward(true, false, false);
    }

    Y_UNIT_TEST(NondeliveryForwardTakesPriorityOverTracking) {
        CheckNondeliveryForward(false, true, false);
        CheckNondeliveryForward(true, true, false);
    }

    Y_UNIT_TEST(NondeliveryForwardMissingFallback) {
        CheckNondeliveryForward(false, false, true);
        CheckNondeliveryForward(true, false, true);
        CheckNondeliveryForward(false, true, true);
        CheckNondeliveryForward(true, true, true);
    }

}
