#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/thread/factory.h>

namespace NKikimr {
    using namespace NActors;

Y_UNIT_TEST_SUITE(TActorTest) {
    TTestActorRuntime::TEgg MakeEgg()
    {
        return
            { new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
    }

    Y_UNIT_TEST(TestHandleEvent) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor(const TActorId& sender)
                : TActor(&TMyActor::StateFunc)
                , Sender(sender)
            {
            }

            STFUNC(StateFunc)
            {
                UNIT_ASSERT_EQUAL_C(ev->Sender, Sender, "sender check");
                UNIT_ASSERT_EQUAL_C(ev->Type, TEvents::TSystem::Wakeup, "ev. type check");
            }

        private:
            const TActorId Sender;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        TActorId actorId = runtime.Register(new TMyActor(sender));
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvWakeup));
    }

    Y_UNIT_TEST(TestDie) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::StateFunc)
            {
            }

            STFUNC(StateFunc)
            {
                UNIT_ASSERT_EQUAL_C(ev->Type, TEvents::TSystem::PoisonPill, "ev. type check");
                PassAway();
            }
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        TActorId actorId = runtime.Register(new TMyActor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPoisonPill));
    }

    Y_UNIT_TEST(TestStateSwitch) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::OldStateFunc)
                , StateChanged(false)
            {
            }

            STFUNC(OldStateFunc)
            {
                UNIT_ASSERT_EQUAL_C(ev->Type, TEvents::THelloWorld::Ping, "ev. type check");
                Become(&TMyActor::NewStateFunc);
            }

            STFUNC(NewStateFunc)
            {
                UNIT_ASSERT_EQUAL_C(ev->Type, TEvents::THelloWorld::Pong, "ev. type check");
                StateChanged = true;
            }

            bool IsStateChanged() {
                return StateChanged;
            }

        private:
            bool StateChanged;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        auto actor = new TMyActor;
        TActorId actorId = runtime.Register(actor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPing));
        UNIT_ASSERT_EQUAL(actor->IsStateChanged(), false);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPong));
        UNIT_ASSERT_EQUAL(actor->IsStateChanged(), true);
    }

    Y_UNIT_TEST(TestSendEvent) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::StateFunc)
            {
            }

            STFUNC(StateFunc)
            {
                Send(ev->Sender, new TEvents::TEvPong());
            }
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        TActorId actorId = runtime.Register(new TMyActor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPing));
        auto events = runtime.CaptureEvents();
        bool passed = false;
        for (const auto& event : events) {
            if (event->GetRecipientRewrite() == sender) {
                UNIT_ASSERT_EQUAL_C(event->Type, TEvents::THelloWorld::Pong, "reply ev. type check");
                passed = true;
                break;
            }
        }
        UNIT_ASSERT(passed);
        runtime.PushEventsFront(events);
    }

    Y_UNIT_TEST(TestScheduleEvent) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::StateFunc)
            {
            }

            STFUNC(StateFunc)
            {
                Y_UNUSED(ev);
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
            }
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
            Y_UNUSED(event);
            deadline = runtime.GetCurrentTime() + delay;
            return false;
        });

        TActorId sender = runtime.AllocateEdgeActor();
        TActorId actorId = runtime.Register(new TMyActor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPing));
        auto scheduledEvents = runtime.CaptureScheduledEvents();
        UNIT_ASSERT_EQUAL_C(scheduledEvents.size(), 1, "check scheduled count");
        UNIT_ASSERT_C(scheduledEvents.begin()->Deadline == (runtime.GetCurrentTime() + TDuration::Seconds(1)), "scheduled delay check");
        UNIT_ASSERT_EQUAL_C(scheduledEvents.begin()->Event->Type, TEvents::TSystem::Wakeup, "scheduled ev. type check");
    }

    Y_UNIT_TEST(TestCreateChildActor) {
        class TChildActor : public TActor<TChildActor> {
        public:
            TChildActor()
                : TActor(&TChildActor::StateFunc)
            {
            }

            STFUNC(StateFunc)
            {
                Y_UNUSED(ev);
            }
        };

        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::StateFunc)
            {
            }

            STFUNC(StateFunc)
            {
                Y_UNUSED(ev);
                ChildId = RegisterWithSameMailbox(new TChildActor());
                Send(ChildId, new TEvents::TEvPing());
            }

            TActorId GetChildId() const {
                return ChildId;
            }

        private:
            TActorId ChildId;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        auto actor = new TMyActor;
        TActorId actorId = runtime.Register(actor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvWakeup));
        auto events = runtime.CaptureEvents();
        bool passed = false;
        for (const auto& event : events) {
            if (event->Recipient == actor->GetChildId()) {
                UNIT_ASSERT_EQUAL_C(event->Type, TEvents::THelloWorld::Ping, "reply ev. type check");
                passed = true;
                break;
            }
        }
        UNIT_ASSERT(passed);
        runtime.PushEventsFront(events);
    }

    Y_UNIT_TEST(TestSendAfterDelay) {
        TMutex syncMutex;

        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor(TMutex *syncMutex)
                : TActor(&TMyActor::StateFunc)
                , CurrentTime(TInstant::MicroSeconds(0))
                , SyncMutex(syncMutex)
            {
            }

            TInstant GetCurrentTime() const {
                return CurrentTime;
            }

            STFUNC(StateFunc)
            {
                Y_ABORT_UNLESS(SyncMutex);

                auto sender = ev->Sender;
                auto selfID = SelfId();
                auto actorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
                TMutex *syncMutex = SyncMutex;

                SystemThreadFactory()->Run([=](){
                    with_lock(*syncMutex) {
                        Sleep(TDuration::MilliSeconds(100));
                        CurrentTime = actorSystem->Timestamp();
                        actorSystem->Send(new IEventHandle(sender, selfID, new TEvents::TEvPong()));
                    }
                });

                SyncMutex = nullptr;
            }

        private:
            TInstant CurrentTime;
            TMutex *SyncMutex;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        try {
            TActorId sender = runtime.AllocateEdgeActor();
            auto myActor = new TMyActor(&syncMutex);
            TActorId actorId = runtime.Register(myActor);
            runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPing));
            runtime.DispatchEvents();
            auto events = runtime.CaptureEvents();
            UNIT_ASSERT_EQUAL_C(events.size(), 1, "check sent count");
            UNIT_ASSERT_EQUAL_C(events[0]->GetRecipientRewrite(), sender, "check recipent");
            UNIT_ASSERT_EQUAL_C(events[0]->Type, TEvents::THelloWorld::Pong, "reply ev. type check");
            UNIT_ASSERT_C(myActor->GetCurrentTime().MilliSeconds() == 0, "check actor system time");
            runtime.PushEventsFront(events);
        }
        catch (...) {
            // small delay to avoid crash when ActorSystem was destroyed
            Sleep(TDuration::MilliSeconds(1000));
            throw;
        }

        with_lock(syncMutex) {}
    }

    Y_UNIT_TEST(TestGetCtxTime) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TThis::StateInit)
                , CurrentTime(TInstant::MicroSeconds(0))
            {
            }

            TInstant GetCurrentTime() const {
                return CurrentTime;
            }

            STFUNC(StateInit)
            {
                switch (ev->GetTypeRewrite()) {
                case TEvents::THelloWorld::Ping:
                    Schedule(TDuration::MilliSeconds(1000), new TEvents::TEvWakeup);
                    Sender = ev->Sender;
                    break;
                case TEvents::TSystem::Wakeup:
                    CurrentTime = TActivationContext::Now();
                    Send(Sender, new TEvents::TEvPong());
                };
            }

        private:
            TActorId Sender;
            TInstant CurrentTime;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant& deadline) {
            if (event->GetTypeRewrite() != TEvents::TSystem::Wakeup)
                return true;

            deadline = runtime.GetCurrentTime() + delay;
            return false;
        });

        runtime.SetScheduledEventsSelectorFunc(&TTestActorRuntimeBase::CollapsedTimeScheduledEventsSelector);
        TActorId sender = runtime.AllocateEdgeActor();
        auto myActor = new TMyActor;
        TActorId actorId = runtime.Register(myActor);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvPing));
        runtime.DispatchEvents();
        auto events = runtime.CaptureEvents();
        UNIT_ASSERT_EQUAL_C(events.size(), 1, "check sent count");
        UNIT_ASSERT_EQUAL_C(events[0]->GetRecipientRewrite(), sender, "check recipent");
        UNIT_ASSERT_EQUAL_C(events[0]->Type, TEvents::THelloWorld::Pong, "reply ev. type check");
        UNIT_ASSERT_C(myActor->GetCurrentTime().MilliSeconds() >= 1000, "check ctx time");
        runtime.PushEventsFront(events);
    }

    Y_UNIT_TEST(TestSendFromAnotherThread) {
        enum {
            EvCounter = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvCounter : public TEventLocal<TEvCounter, EvCounter> {
            TEvCounter(ui32 counter)
                : Counter(counter)
            {}

            const ui32 Counter;
        };

        class TConsumerActor : public TActor<TConsumerActor> {
        public:
            TConsumerActor(ui32 count)
                : TActor(&TConsumerActor::StateFunc)
                , Count(count)
                , ExpectedCounter(0)
            {
            }

            bool IsComplete() const {
                return ExpectedCounter == Count;
            }

            STFUNC(StateFunc)
            {
                switch (ev->GetTypeRewrite()) {
                case EvCounter:
                    ui32 eventCounter = ev->CastAsLocal<TEvCounter>()->Counter;
                    UNIT_ASSERT_EQUAL_C(ExpectedCounter, eventCounter, "check counter");
                    ExpectedCounter = eventCounter + 1;
                    break;
                }
            }

        private:
            const ui32 Count;
            ui32 ExpectedCounter;
        };

        class TProducerActor : public TActor<TProducerActor> {
        public:
            TProducerActor(ui32 count, const TVector<TActorId>& recipents)
                : TActor(&TProducerActor::StateFunc)
                , Count(count)
                , Recipents(recipents)
            {
            }

            ~TProducerActor() {
                if (Thread)
                    Thread->Join();
            }

            STFUNC(StateFunc)
            {
                Y_UNUSED(ev);
                ActorSystem = TlsActivationContext->ExecutorThread.ActorSystem;
                Thread.Reset(new TThread(&TProducerActor::ThreadProc, this));
                Thread->Start();
            }

            static void* ThreadProc(void* param) {
                TProducerActor* actor = (TProducerActor*)(param);
                for (ui32 i = 0; i < actor->Count; ++i) {
                    for (const auto& recip : actor->Recipents) {
                        actor->ActorSystem->Send(new IEventHandle(recip, actor->SelfId(), new TEvCounter(i)));
                        if ((i % (1 + rand() % 100)) == 0) {
                            Sleep(TDuration::MilliSeconds(1 + rand() % 10));
                        }
                    }
                }

                return nullptr;
            }

        private:
            const ui32 Count;
            const TVector<TActorId> Recipents;
            TAutoPtr<TThread> Thread;
            TActorSystem* ActorSystem = nullptr;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());

        try {
            TActorId sender = runtime.AllocateEdgeActor();
            ui32 count = 1000;
            ui32 consumersCount = 4;
            TVector<TConsumerActor*> consumers;
            TVector<TActorId> consumerIds;
            for (ui32 i = 0; i < consumersCount; ++i) {
                auto consumerActor = new TConsumerActor(count);
                TActorId consumerId = runtime.Register(consumerActor);
                consumers.push_back(consumerActor);
                consumerIds.push_back(consumerId);
            }

            auto producerActor = new TProducerActor(count, consumerIds);
            TActorId producerId = runtime.Register(producerActor);
            runtime.Send(new IEventHandle(producerId, sender, new TEvents::TEvPing));
            runtime.SetObserverFunc([](TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(event);
                return TTestActorRuntime::EEventAction::PROCESS;
            });

            for (;;) {
                TDispatchOptions options;
                options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition((ui32)EvCounter));
                runtime.DispatchEvents(options);
                bool hasAlive = false;
                for (auto consumer : consumers) {
                    if (!consumer->IsComplete())
                        hasAlive = true;
                }

                if (!hasAlive)
                    break;
            }
        }
        catch (...) {
            throw;
        }
    }

    Y_UNIT_TEST(TestScheduleReaction) {
        class TMyActor : public TActor<TMyActor> {
        public:
            TMyActor()
                : TActor(&TMyActor::StateFunc)
            {}

            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvBootstrap, Handle);
                    HFunc(TEvents::TEvCompleted, Handle);
                    HFunc(TEvents::TEvPing, Handle);
                    HFunc(TEvents::TEvPong, Handle);
                    default:
                        Y_ABORT("unexpected event");
                }
            }

            void Handle(TEvents::TEvBootstrap::TPtr& ev, const TActorContext& ctx) {
                ctx.Send(ev->Sender, new TEvents::TEvCompleted());
            }

            void Handle(TEvents::TEvPing::TPtr&, const TActorContext& ctx) {
                ctx.Schedule(TDuration::MilliSeconds(400), new TEvents::TEvPong());
                ctx.Schedule(TDuration::MilliSeconds(500), new TEvents::TEvCompleted());
            }

            void Handle(TEvents::TEvPong::TPtr&, const TActorContext&) {
            }

            void Handle(TEvents::TEvCompleted::TPtr&, const TActorContext&) {
                CompletedReceived = true;
            }

            bool CompletedReceived = false;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId sender = runtime.AllocateEdgeActor();
        TMyActor* myActor = new TMyActor;
        TActorId actorId = runtime.Register(myActor);
        runtime.EnableScheduleForActor(actorId);
        runtime.Send(new IEventHandle(actorId, sender, new TEvents::TEvBootstrap()));
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(handle));
        runtime.Send(new IEventHandle(actorId, TActorId(), new TEvents::TEvPing()));
        runtime.Schedule(new IEventHandle(sender, TActorId(), new TEvents::TEvWakeup()),
            TDuration::MilliSeconds(1000000));
        do {
            runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
        } while (handle->GetRecipientRewrite() != sender);
        UNIT_ASSERT(myActor->CompletedReceived);
    }

    Y_UNIT_TEST(TestFilteredGrab) {
        enum EEv {
            EvCounter = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvCounter : public TEventLocal<TEvCounter, EvCounter> {
            const ui32 Index;

            TEvCounter(ui32 index)
                : Index(index)
            {};
        };

        class TCountingActor : public TActorBootstrapped<TCountingActor> {
        private:
            TVector<TActorId> Targets;
            ui32 Counter = 0;

        public:
            TCountingActor(TVector<TActorId> targets)
                : Targets(targets)
            {}

            void Bootstrap(const TActorContext& ctx) {
                ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
                Become(&TThis::StateWork);
            }

            void Handle(const TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
                size_t index = Counter++ % Targets.size();
                ctx.Send(Targets[index], new TEvCounter(Counter));
                ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            }

            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvents::TEvWakeup, Handle);
                    default:
                        Y_ABORT("unexpected event");
                }
            }
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());
        TActorId edge1 = runtime.AllocateEdgeActor();
        TActorId edge2 = runtime.AllocateEdgeActor();
        TActorId edge3 = runtime.AllocateEdgeActor();
        TActorId countingActor = runtime.Register(new TCountingActor({edge1, edge2}));
        runtime.EnableScheduleForActor(countingActor);

        // Ignores edge1 event
        {
            auto event = runtime.GrabEdgeEvent<TEvCounter>(edge2);
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, edge2);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Index, 2u);
        }

        // Ignores another edge1 event
        {
            auto event = runtime.GrabEdgeEventRethrow<TEvCounter>(edge2);
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, edge2);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Index, 4u);
        }

        // There are no edge3 events, so timeout hits
        {
            auto event = runtime.GrabEdgeEvent<TEvCounter>(edge3, TDuration::Seconds(5));
            UNIT_ASSERT(!event);
        }

        // Now we take skipped event for edge1
        {
            auto event = runtime.GrabEdgeEvent<TEvCounter>(edge1);
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, edge1);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Index, 1u);
        }

        // And another event for edge1
        {
            auto event = runtime.GrabEdgeEventRethrow<TEvCounter>(edge1);
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, edge1);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Index, 3u);
        }

        // Grab event using complex filters
        {
            auto event = runtime.GrabEdgeEventIf<TEvCounter>(
                {edge1, edge2},
                [](const auto& ev) {
                    const auto* msg = ev->Get();
                    return msg->Index > 10 && msg->Index % 2 == 0;
                });
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, edge2);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Index, 12u);
        }
    }

    Y_UNIT_TEST(TestWaitFuture) {
        enum EEv {
            EvTrigger = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvTrigger : public TEventLocal<TEvTrigger, EvTrigger> {
            TEvTrigger() = default;
        };

        class TTriggerActor : public TActorBootstrapped<TTriggerActor> {
        public:
            TTriggerActor(NThreading::TPromise<void> promise)
                : Promise(std::move(promise))
            {}

            void Bootstrap() {
                Schedule(TDuration::Seconds(1), new TEvTrigger);
                Become(&TThis::StateWork);
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvTrigger, Handle);
                }
            }

            void Handle(TEvTrigger::TPtr&) {
                Promise.SetValue();
                PassAway();
            }

        private:
            NThreading::TPromise<void> Promise;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        NThreading::TFuture<void> future = promise.GetFuture();

        auto actor = runtime.Register(new TTriggerActor(std::move(promise)));
        runtime.EnableScheduleForActor(actor);

        runtime.WaitFuture(std::move(future));
    }

    Y_UNIT_TEST(TestWaitFor) {
        enum EEv {
            EvTrigger = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvTrigger : public TEventLocal<TEvTrigger, EvTrigger> {
            TEvTrigger() = default;
        };

        class TTriggerActor : public TActorBootstrapped<TTriggerActor> {
        public:
            TTriggerActor(int* ptr)
                : Ptr(ptr)
            {}

            void Bootstrap() {
                Schedule(TDuration::Seconds(1), new TEvTrigger);
                Become(&TThis::StateWork);
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvTrigger, Handle);
                }
            }

            void Handle(TEvTrigger::TPtr&) {
                *Ptr = 42;
                PassAway();
            }

        private:
            int* Ptr;
        };

        TTestActorRuntime runtime;
        runtime.Initialize(MakeEgg());

        int value = 0;
        auto actor = runtime.Register(new TTriggerActor(&value));
        runtime.EnableScheduleForActor(actor);

        runtime.WaitFor("value = 42", [&]{ return value == 42; });
        UNIT_ASSERT_VALUES_EQUAL(value, 42);
    }

    Y_UNIT_TEST(TestBlockEvents) {
        enum EEv {
            EvTrigger = EventSpaceBegin(TEvents::ES_PRIVATE)
        };

        struct TEvTrigger : public TEventLocal<TEvTrigger, EvTrigger> {
            int Value;

            TEvTrigger(int value)
                : Value(value)
            {}
        };

        class TTargetActor : public TActorBootstrapped<TTargetActor> {
        public:
            TTargetActor(std::vector<int>* ptr)
                : Ptr(ptr)
            {}

            void Bootstrap() {
                Become(&TThis::StateWork);
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvTrigger, Handle);
                }
            }

            void Handle(TEvTrigger::TPtr& ev) {
                Ptr->push_back(ev->Get()->Value);
            }

        private:
            std::vector<int>* Ptr;
        };

        class TSourceActor : public TActorBootstrapped<TSourceActor> {
        public:
            TSourceActor(const TActorId& target)
                : Target(target)
            {}

            void Bootstrap() {
                Become(&TThis::StateWork);
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
            }

        private:
            STFUNC(StateWork) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvents::TEvWakeup, Handle);
                }
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                Send(Target, new TEvTrigger(++Counter));
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
            }

        private:
            TActorId Target;
            int Counter = 0;
        };

        TTestActorRuntime runtime(2);
        runtime.Initialize(MakeEgg());

        std::vector<int> values;
        auto target = runtime.Register(new TTargetActor(&values), /* nodeIdx */ 1);
        auto source = runtime.Register(new TSourceActor(target), /* nodeIdx */ 1);
        runtime.EnableScheduleForActor(source);

        TBlockEvents<TEvTrigger> block(runtime, [&](TEvTrigger::TPtr& ev){ return ev->GetRecipientRewrite() == target; });
        runtime.WaitFor("blocked 3 events", [&]{ return block.size() >= 3; });
        UNIT_ASSERT_VALUES_EQUAL(block.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 0u);

        block.Unblock(2);
        UNIT_ASSERT_VALUES_EQUAL(block.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 0u);

        runtime.WaitFor("blocked 1 more event", [&]{ return block.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(block.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(values.at(0), 1);
        UNIT_ASSERT_VALUES_EQUAL(values.at(1), 2);
        values.clear();

        block.Stop();
        runtime.WaitFor("processed 2 more events", [&]{ return values.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(block.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(values.at(0), 5);
        UNIT_ASSERT_VALUES_EQUAL(values.at(1), 6);
        values.clear();

        block.Unblock();
        UNIT_ASSERT_VALUES_EQUAL(block.size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 0u);
        runtime.WaitFor("processed 3 more events", [&]{ return values.size() >= 3; });
        UNIT_ASSERT_VALUES_EQUAL(values.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(values.at(0), 3);
        UNIT_ASSERT_VALUES_EQUAL(values.at(1), 4);
        UNIT_ASSERT_VALUES_EQUAL(values.at(2), 7);
    }
}

}
