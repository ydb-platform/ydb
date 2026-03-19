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

using namespace NActors;
using namespace NActors::NTracing;

Y_UNIT_TEST_SUITE(TracerTest) {

    Y_UNIT_TEST(EventSize) {
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TTraceEvent), 32u);
    }

    Y_UNIT_TEST(SerializeDeserializeRoundTrip) {
        TTraceChunk chunk;
        chunk.ActivityDict = {{0, "ACTOR_A"}, {3, "ACTOR_B"}, {10, "ACTOR_C"}};
        chunk.EventNamesDict = {{100, "TEvRequest"}, {200, "TEvResponse"}};

        TTraceEvent ev1{};
        ev1.Timestamp = 1000000;
        ev1.Actor1 = 42;
        ev1.Type = static_cast<ui8>(ETraceEventType::New);
        chunk.Events.push_back(ev1);

        TTraceEvent ev2{};
        ev2.Timestamp = 1000010;
        ev2.Actor1 = 1;
        ev2.Actor2 = 42;
        ev2.Aux = 100;
        ev2.Type = static_cast<ui8>(ETraceEventType::SendLocal);
        chunk.Events.push_back(ev2);

        TTraceEvent ev3{};
        ev3.Timestamp = 1000020;
        ev3.Actor1 = 1;
        ev3.Actor2 = 42;
        ev3.Aux = 100;
        ev3.Extra = 3;
        ev3.Type = static_cast<ui8>(ETraceEventType::ReceiveLocal);
        chunk.Events.push_back(ev3);

        TTraceEvent ev4{};
        ev4.Timestamp = 1000030;
        ev4.Actor1 = 42;
        ev4.Type = static_cast<ui8>(ETraceEventType::Die);
        chunk.Events.push_back(ev4);

        auto buf = SerializeTrace(chunk, 1);
        UNIT_ASSERT(buf.Size() > 0);

        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));

        UNIT_ASSERT_VALUES_EQUAL(nodeId, 1u);
        UNIT_ASSERT_VALUES_EQUAL(restored.ActivityDict.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), 4u);
        UNIT_ASSERT_VALUES_EQUAL(restored.EventNamesDict.size(), 2u);

        UNIT_ASSERT_VALUES_EQUAL(restored.Events[0].Type, static_cast<ui8>(ETraceEventType::New));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[1].Type, static_cast<ui8>(ETraceEventType::SendLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[2].Type, static_cast<ui8>(ETraceEventType::ReceiveLocal));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events[3].Type, static_cast<ui8>(ETraceEventType::Die));
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
            UNIT_ASSERT_C(ev.Timestamp > 0, "Event has zero timestamp");
            auto type = static_cast<ETraceEventType>(ev.Type);
            switch (type) {
                case ETraceEventType::SendLocal:
                    sendCount++;
                    UNIT_ASSERT(ev.Actor2 != 0);
                    break;
                case ETraceEventType::ReceiveLocal:
                    receiveCount++;
                    UNIT_ASSERT(ev.Actor2 != 0);
                    break;
                case ETraceEventType::New:
                    newCount++;
                    UNIT_ASSERT(ev.Actor1 != 0);
                    break;
                case ETraceEventType::Die:
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

        auto buf = SerializeTrace(chunk, 1);
        TTraceChunk restored;
        ui32 nodeId = 0;
        UNIT_ASSERT(DeserializeTrace(buf, restored, nodeId));
        UNIT_ASSERT_VALUES_EQUAL(restored.Events.size(), chunk.Events.size());
    }
}
