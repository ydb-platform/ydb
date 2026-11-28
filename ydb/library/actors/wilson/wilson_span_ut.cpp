#include "wilson_span.h"
#include "wilson_uploader.h"
#include "wilson_actor_tracing.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NWilson;

Y_UNIT_TEST_SUITE(TSpan) {
    Y_UNIT_TEST(ActorHooksPreserveMailboxToHandlerCausality) {
        class TEcho : public NActors::TActorBootstrapped<TEcho> {
        public:
            explicit TEcho(NActors::TActorId edge) : Edge(edge) {}
            void Bootstrap() { Become(&TEcho::StateWork); }
            STATEFN(StateWork) {
                if (ev->GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType) {
                    Send(Edge, new NActors::TEvents::TEvWakeup());
                    PassAway();
                }
            }
            const NActors::TActorId Edge;
        };
        NActors::TTestActorRuntimeBase runtime(1, true);
        runtime.SetupNodeSubSystems = [](ui32, NActors::TActorSystemSetup* setup) {
            setup->EventTracing = CreateActorEventTracing();
        };
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        const auto uploader = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeWilsonUploaderId(), uploader);
        const auto actor = runtime.Register(new TEcho(edge));
        runtime.Send(new NActors::IEventHandle(actor, edge, new NActors::TEvents::TEvWakeup(),
            0, 0, nullptr, TTraceId::NewTraceId(15, 4095)), 0, true);
        runtime.GrabEdgeEvent<NActors::TEvents::TEvWakeup>(edge);
        NTraceProto::Span submit, handler;
        for (size_t i = 0; submit.name().empty() || handler.name().empty(); ++i) {
            UNIT_ASSERT(i < 8);
            auto event = runtime.GrabEdgeEvent<TEvWilson>(uploader);
            const auto& span = event->Get()->Span;
            if (span.name() == "Actor.MailboxSubmit") { submit = span; }
            if (span.name() == "Actor.Handler") { handler = span; }
        }
        UNIT_ASSERT_VALUES_EQUAL(handler.trace_id(), submit.trace_id());
        UNIT_ASSERT_VALUES_EQUAL(handler.parent_span_id(), submit.span_id());
        bool wall = false, cpu = false, recipient = false, context = false;
        for (const auto& attribute : handler.attributes()) {
            wall |= attribute.key() == "ydb.handler.wall_us";
            cpu |= attribute.key() == "ydb.handler.thread_cpu_us";
            recipient |= attribute.key() == "ydb.actor.recipient_x1";
            context |= attribute.key() == "ydb.actor.incoming_context";
        }
        UNIT_ASSERT(wall && cpu && recipient && context);
        UNIT_ASSERT_VALUES_EQUAL(submit.events(0).name(), "mailbox push entered");
        UNIT_ASSERT_VALUES_EQUAL(submit.events(1).name(), "mailbox push returned");
    }

    Y_UNIT_TEST(ConstructTerminatedTimestamps) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeWilsonUploaderId(), edge);

        const auto parent = TTraceId::NewTraceId(15, 4095);
        constexpr ui64 start = 1'700'000'000'000'000'123;
        const struct {
            ui64 End;
            ui64 ExpectedEnd;
        } cases[] = {
            {start + 1'234, start + 1'234},
            {start - 1'234, start},
        };
        for (const auto& test : cases) {
            auto span = TSpan::ConstructTerminated(parent, parent.Span(15), start, test.End,
                NTraceProto::Status::STATUS_CODE_OK, "Terminated", runtime.GetActorSystem(0));
            span.End();
            const auto event = runtime.GrabEdgeEvent<TEvWilson>(edge);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.start_time_unix_nano(), start);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.end_time_unix_nano(), test.ExpectedEnd);
        }
    }
}
