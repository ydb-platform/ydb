#include "wilson_span.h"
#include "wilson_uploader.h"

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NWilson;

Y_UNIT_TEST_SUITE(TSpan) {
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
