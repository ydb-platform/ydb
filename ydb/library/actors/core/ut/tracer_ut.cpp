#include <ydb/library/actors/core/tracer.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/trace_data/trace_data.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/generic/hash_set.h>

using namespace NActors;
using namespace NActors::NTracing;

Y_UNIT_TEST_SUITE(TracerTest) {

    Y_UNIT_TEST(EventSize) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TTraceEvent), 36u);
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTrip) {
        TTraceChunk chunk;
        chunk.StartTimestampUs = 1700000000000000ULL;
        chunk.ActivityDict = {{0, "ACTOR_A"}, {3, "ACTOR_B"}, {10, "ACTOR_C"}};
        chunk.EventNamesDict = {{100, "TEvRequest"}, {200, "TEvResponse"}};

        TTraceEvent ev1{};
        ev1.DeltaUs = 1000000;
        ev1.Actor1 = 42;
        ev1.Type = static_cast<ui8>(ETraceEventType::New);
        chunk.Events.push_back(ev1);

        TTraceEvent ev2{};
        ev2.DeltaUs = 1000010;
        ev2.Actor1 = 1;
        ev2.Actor2 = 42;
        ev2.Aux = 100;
        ev2.Type = static_cast<ui8>(ETraceEventType::SendLocal);
        ev2.HandlePtr = 0x11111111;
        chunk.Events.push_back(ev2);

        TTraceEvent ev3{};
        ev3.DeltaUs = 1000020;
        ev3.Actor1 = 1;
        ev3.Actor2 = 42;
        ev3.Aux = 100;
        ev3.Extra = 3;
        ev3.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
        ev3.HandlePtr = 0x11111111;
        chunk.Events.push_back(ev3);

        TTraceEvent ev4{};
        ev4.DeltaUs = 1000030;
        ev4.Actor1 = 42;
        ev4.Type = static_cast<ui8>(ETraceEventType::Die);
        chunk.Events.push_back(ev4);

        TTraceEvent ev5{};
        ev5.DeltaUs = 1000040;
        ev5.Actor1 = 0x22222222;
        ev5.Actor2 = 42;
        ev5.Aux = 100;
        ev5.Extra = 3;
        ev5.Type = static_cast<ui8>(ETraceEventType::ForwardLocal);
        ev5.HandlePtr = 0x11111111;
        chunk.Events.push_back(ev5);

        auto buf = SerializeTrace(chunk, 1);
        UNIT_ASSERT(buf.Size() > 0);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));

        UNIT_ASSERT_VALUES_EQUAL(nodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.StartTimestampUs, chunk.StartTimestampUs);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), 5u);
        UNIT_ASSERT_VALUES_EQUAL(restored.EventNamesDict.size(), 2u);

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Type, static_cast<ui8>(ETraceEventType::New));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Type, static_cast<ui8>(ETraceEventType::SendLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].Type, static_cast<ui8>(ETraceEventType::ReceiveLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[3].Type, static_cast<ui8>(ETraceEventType::Die));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].Type, static_cast<ui8>(ETraceEventType::ForwardLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].DeltaUs, 1000000u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].DeltaUs, 1000040u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].HandlePtr, 0x11111111ull);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].HandlePtr, 0x11111111ull);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[4].Actor1, 0x22222222ull);
    }

    Y_UNIT_TEST(DeserializeRejectsBadMagic) {
        TBuffer buf;
        buf.Append("GARBAGE_DATA_HERE_1234567890", 28);
        TTraceChunk chunk;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(buf, chunk, nodeId));
    }

    Y_UNIT_TEST(DeserializeRejectsTruncatedData) {
        TTraceChunk chunk;
        chunk.Events.push_back(TTraceEvent{});
        auto buf = SerializeTrace(chunk, 1);
        TBuffer truncated;
        truncated.Append(buf.Data(), buf.Size() / 2);
        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(!DeserializeTrace(truncated, restored, nodeId));
    }

    Y_UNIT_TEST(EmptyTrace) {
        TTraceChunk chunk;
        auto buf = SerializeTrace(chunk, 5);
        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(nodeId, 5u);
        UNIT_ASSERT(restored.Events.empty());
    }

    struct TEvPing : TEventLocal<TEvPing, TEvents::THelloWorld::Ping> {};
    struct TEvPong : TEventLocal<TEvPong, TEvents::THelloWorld::Pong> {};

    class TPongActor : public TActor<TPongActor> {
    public:
        TPongActor()
            : TActor(&TThis::StateWork)
        {}

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPing, Handle);
            }
        }

        void Handle(TEvPing::TPtr& ev, const TActorContext& ctx) {
            ctx.Send(ev->Sender, new TEvPong());
        }
    };

    class TPingActor : public TActor<TPingActor> {
    public:
        TPingActor(TActorId pongActorId, TManualEvent& done, int count)
            : TActor(&TThis::StateWork)
            , PongActorId(pongActorId)
            , Done(done)
            , Remaining(count)
        {}

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPong, Handle);
                CFunc(TEvents::TSystem::Bootstrap, Bootstrap);
            }
        }

        void Bootstrap(const TActorContext& ctx) {
            for (int i = 0; i < Remaining; ++i) {
                ctx.Send(PongActorId, new TEvPing());
            }
        }

        void Handle(TEvPong::TPtr&, const TActorContext&) {
            if (--Remaining <= 0) {
                Done.Signal();
            }
        }

    private:
        TActorId PongActorId;
        TManualEvent& Done;
        int Remaining;
    };

    class TForwardActor : public TActor<TForwardActor> {
    public:
        explicit TForwardActor(TActorId target)
            : TActor(&TThis::StateWork)
            , Target(target)
        {}

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvPing, Handle);
            }
        }

        void Handle(TEvPing::TPtr& ev, const TActorContext&) {
            Forward(ev, Target);
        }

    private:
        TActorId Target;
    };

    Y_UNIT_TEST(RealActorSystemCollectsEvents) {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup());
        setup->NodeId = 1;
        setup->ExecutorsCount = 2;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[2]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 2, 20));
        setup->Executors[1].Reset(new TBasicExecutorPool(1, 2, 20));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 0)));

        auto logSettings = MakeIntrusive<NLog::TSettings>(
            TActorId(1, "logger"),
            0,
            NLog::PRI_WARN);
        logSettings->TracerSettings.AutoStart = true;
        logSettings->TracerSettings.MaxBufferSizePerThread = 4096;

        TActorSystem actorSystem(setup, nullptr, logSettings);
        actorSystem.Start();

        auto pongActorId = actorSystem.Register(new TPongActor());

        TManualEvent done;
        const int messageCount = 100;
        auto pingActorId = actorSystem.Register(new TPingActor(pongActorId, done, messageCount));
        actorSystem.Send(pingActorId, new TEvents::TEvBootstrap());

        done.WaitT(TDuration::Seconds(5));

        auto* tracer = actorSystem.GetActorTracer();
        UNIT_ASSERT(tracer != nullptr);

        tracer->Stop();
        auto chunk = tracer->GetTraceData();

        actorSystem.Stop();
        actorSystem.Cleanup();

        UNIT_ASSERT_C(chunk.Events.size() > 0,
            "No events collected! Events: " << chunk.Events.size());

        size_t sendCount = 0, receiveCount = 0, newCount = 0;
        for (const auto& ev : chunk.Events) {
            auto type = static_cast<ETraceEventType>(ev.Type);
            switch (type) {
                case ETraceEventType::SendLocal:
                    sendCount++;
                    UNIT_ASSERT(ev.Actor2 != 0);
                    UNIT_ASSERT(ev.HandlePtr != 0);
                    break;
                case ETraceEventType::ReceiveLocal:
                    receiveCount++;
                    UNIT_ASSERT(ev.Actor2 != 0);
                    UNIT_ASSERT(ev.HandlePtr != 0);
                    break;
                case ETraceEventType::New:
                    newCount++;
                    UNIT_ASSERT(ev.Actor1 != 0);
                    break;
                case ETraceEventType::Die:
                    UNIT_ASSERT(ev.Actor1 != 0);
                    break;
                case ETraceEventType::ForwardLocal:
                    UNIT_ASSERT(ev.HandlePtr != 0);
                    UNIT_ASSERT(ev.Actor1 != 0);
                    break;
            }
        }

        UNIT_ASSERT_C(sendCount >= (size_t)messageCount,
            "Expected at least " << messageCount << " sends, got " << sendCount);
        UNIT_ASSERT_C(receiveCount >= (size_t)messageCount,
            "Expected at least " << messageCount << " receives, got " << receiveCount);
        UNIT_ASSERT_C(newCount >= 2,
            "Expected at least 2 New events (ping + pong actors), got " << newCount);
        UNIT_ASSERT_C(!chunk.EventNamesDict.empty(),
            "EventNamesDict should not be empty");

        THashSet<ui8> threadIdxSet;
        for (const auto& ev : chunk.Events) {
            threadIdxSet.insert(ev.Flags);
        }
        UNIT_ASSERT_C(threadIdxSet.size() >= 2,
            "Expected events from at least 2 threads, got " << threadIdxSet.size());

        size_t sendsWithActivityType = 0;
        for (const auto& ev : chunk.Events) {
            if (static_cast<ETraceEventType>(ev.Type) == ETraceEventType::SendLocal && ev.Extra != 0) {
                sendsWithActivityType++;
            }
        }
        UNIT_ASSERT_C(sendsWithActivityType > 0,
            "Expected at least some Send events with non-zero Extra (sender ActivityType)");

        auto buf = SerializeTrace(chunk, 1);
        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), chunk.Events.size());

        THashSet<ui32> eventTypesInTrace;
        for (const auto& ev : chunk.Events) {
            auto type = static_cast<ETraceEventType>(ev.Type);
            if (type == ETraceEventType::SendLocal || type == ETraceEventType::ReceiveLocal) {
                if (ev.Aux != 0) {
                    eventTypesInTrace.insert(ev.Aux);
                }
            }
        }
        for (ui32 eventType : eventTypesInTrace) {
            UNIT_ASSERT_C(chunk.EventNamesDict.contains(eventType),
                "Event type " << eventType << " should be in EventNamesDict");
        }
    }

    Y_UNIT_TEST(ForwardCreatesHandleRemapEvent) {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup());
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 20));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 0)));

        auto logSettings = MakeIntrusive<NLog::TSettings>(
            TActorId(1, "logger"),
            0,
            NLog::PRI_WARN);
        logSettings->TracerSettings.AutoStart = true;
        logSettings->TracerSettings.MaxBufferSizePerThread = 4096;

        TActorSystem actorSystem(setup, nullptr, logSettings);
        actorSystem.Start();

        auto pongActorId = actorSystem.Register(new TPongActor());
        auto forwardActorId = actorSystem.Register(new TForwardActor(pongActorId));

        TManualEvent done;
        const int messageCount = 8;
        auto pingActorId = actorSystem.Register(new TPingActor(forwardActorId, done, messageCount));
        actorSystem.Send(pingActorId, new TEvents::TEvBootstrap());

        done.WaitT(TDuration::Seconds(5));

        auto* tracer = actorSystem.GetActorTracer();
        UNIT_ASSERT(tracer != nullptr);

        tracer->Stop();
        auto chunk = tracer->GetTraceData();

        actorSystem.Stop();
        actorSystem.Cleanup();

        THashSet<ui64> sendHandles;
        THashSet<ui64> receiveHandles;
        TVector<TTraceEvent> forwardEvents;

        for (const auto& ev : chunk.Events) {
            const auto type = static_cast<ETraceEventType>(ev.Type);
            if (type == ETraceEventType::SendLocal) {
                sendHandles.insert(ev.HandlePtr);
            } else if (type == ETraceEventType::ReceiveLocal) {
                receiveHandles.insert(ev.HandlePtr);
            } else if (type == ETraceEventType::ForwardLocal) {
                forwardEvents.push_back(ev);
            }
        }

        UNIT_ASSERT_C(!forwardEvents.empty(), "Expected at least one ForwardLocal event");

        bool foundMatchedRemap = false;
        for (const auto& ev : forwardEvents) {
            UNIT_ASSERT_VALUES_EQUAL(ev.Aux, TEvPing::EventType);
            UNIT_ASSERT_VALUES_EQUAL(ev.Actor2, pongActorId.LocalId());
            if (receiveHandles.contains(ev.HandlePtr) && sendHandles.contains(ev.Actor1)) {
                foundMatchedRemap = true;
            }
        }

        UNIT_ASSERT_C(foundMatchedRemap,
            "Expected ForwardLocal remap to connect received old handle with sent new handle");
    }

    Y_UNIT_TEST(BuffersResetOnStart) {
        THolder<TActorSystemSetup> setup(new TActorSystemSetup());
        setup->NodeId = 1;
        setup->ExecutorsCount = 1;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
        setup->Executors[0].Reset(new TBasicExecutorPool(0, 1, 10));
        setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(512, 0)));

        auto logSettings = MakeIntrusive<NLog::TSettings>(TActorId(1, "logger"), 0, NLog::PRI_WARN);
        logSettings->TracerSettings.AutoStart = false;
        logSettings->TracerSettings.MaxBufferSizePerThread = 4096;

        TActorSystem actorSystem(setup, nullptr, logSettings);
        actorSystem.Start();

        auto* tracer = actorSystem.GetActorTracer();
        UNIT_ASSERT(tracer != nullptr);

        UNIT_ASSERT(tracer->Start());
        auto pongActorId = actorSystem.Register(new TPongActor());
        TManualEvent done1;
        auto pingActorId = actorSystem.Register(new TPingActor(pongActorId, done1, 20));
        actorSystem.Send(pingActorId, new TEvents::TEvBootstrap());
        done1.WaitT(TDuration::Seconds(5));

        tracer->Stop();
        auto chunk1 = tracer->GetTraceData();
        UNIT_ASSERT_C(!chunk1.Events.empty(), "First session should have events");
        const size_t session1Count = chunk1.Events.size();
        const ui64 session1Start = chunk1.StartTimestampUs;
        UNIT_ASSERT_C(session1Start != 0, "Session 1 StartTimestampUs must be populated");

        Sleep(TDuration::MilliSeconds(10));

        UNIT_ASSERT(tracer->Start());
        TManualEvent done2;
        auto pingActorId2 = actorSystem.Register(new TPingActor(pongActorId, done2, 3));
        actorSystem.Send(pingActorId2, new TEvents::TEvBootstrap());
        done2.WaitT(TDuration::Seconds(5));

        tracer->Stop();
        auto chunk2 = tracer->GetTraceData();
        UNIT_ASSERT_C(!chunk2.Events.empty(), "Second session should have events");

        UNIT_ASSERT_C(chunk2.StartTimestampUs > session1Start,
            "Session 2 StartTimestampUs (" << chunk2.StartTimestampUs
            << ") should be later than session 1 (" << session1Start << ")");

        UNIT_ASSERT_C(chunk2.Events.size() < session1Count,
            "Session 2 chunk size (" << chunk2.Events.size()
            << ") must be smaller than session 1 (" << session1Count
            << "); looks like buffers were not reset on Start()");

        for (const auto& ev : chunk2.Events) {
            UNIT_ASSERT_C(ev.DeltaUs < 10u * 1000u * 1000u,
                "Event DeltaUs " << ev.DeltaUs << " unreasonably large for session 2");
        }

        actorSystem.Stop();
        actorSystem.Cleanup();
    }
}
