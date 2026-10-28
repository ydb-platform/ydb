#include "wilson_span.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>

#include <chrono>
#include <thread>

using namespace NWilson;

namespace {

const TTraceSnapshot::TOtelSpan& GetSpan(const TTraceSnapshot& snapshot, TStringBuf name) {
    for (const auto& span : snapshot.Spans) {
        if (span.name() == name) {
            return span;
        }
    }
    UNIT_FAIL("Missing span " << name);
    Y_ABORT();
}

} // namespace

Y_UNIT_TEST_SUITE(TSpan) {
    Y_UNIT_TEST(TimestampsDoNotDependOnCycleCalibration) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = new TFakeWilsonUploader();
        runtime.RegisterService(MakeWilsonUploaderId(), runtime.Register(uploader));

        ui64 eventBefore, eventAfter, endBefore, endAfter;
        {
            // A wrong calibration must not move ends and events relative to starts.
            SetCyclesPerSecond(GetCyclesPerMillisecond() * 1000 / 2);
            Y_DEFER { SetCyclesPerSecond(0); };

            TSpan root(15, TTraceId::NewTraceId(15, 4095), "Root", EFlags::NONE, runtime.GetActorSystem(0));
            auto first = root.CreateChild(15, "First");
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            eventBefore = TSpan::NowUnixNano();
            first.Event("Ready");
            eventAfter = TSpan::NowUnixNano();
            endBefore = TSpan::NowUnixNano();
            first.EndOk();
            endAfter = TSpan::NowUnixNano();
            auto next = root.CreateChild(15, "Next");
            next.EndOk();
            root.EndOk();
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        const auto& root = GetSpan(*uploader, "Root");
        const auto& first = GetSpan(*uploader, "First");
        const auto& next = GetSpan(*uploader, "Next");
        UNIT_ASSERT_LE(root.start_time_unix_nano(), first.start_time_unix_nano());
        UNIT_ASSERT_LE(first.end_time_unix_nano(), next.start_time_unix_nano());
        UNIT_ASSERT_LE(next.end_time_unix_nano(), root.end_time_unix_nano());
        UNIT_ASSERT_VALUES_EQUAL(first.events_size(), 1);
        UNIT_ASSERT_LE(eventBefore, first.events(0).time_unix_nano());
        UNIT_ASSERT_LE(first.events(0).time_unix_nano(), eventAfter);
        UNIT_ASSERT_LE(endBefore, first.end_time_unix_nano());
        UNIT_ASSERT_LE(first.end_time_unix_nano(), endAfter);
    }

#if defined(_linux_)
    Y_UNIT_TEST(ClockHasNanosecondResolution) {
        // Microsecond readings would tie the boundaries of short spans.
        bool subMicrosecond = false;
        for (size_t i = 0; i < 1000 && !subMicrosecond; ++i) {
            subMicrosecond = TSpan::NowUnixNano() % 1000 != 0;
        }
        UNIT_ASSERT(subMicrosecond);
    }
#endif

    Y_UNIT_TEST(ConstructTerminatedKeepsTimestamps) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        auto* uploader = new TFakeWilsonUploader();
        runtime.RegisterService(MakeWilsonUploaderId(), runtime.Register(uploader));

        const auto parent = TTraceId::NewTraceId(15, 4095);
        const auto id = parent.Span(15);
        const ui64 start = TSpan::NowUnixNano();
        const ui64 end = start + 1'234;
        // Off the actor system's threads the span needs the actor system explicitly.
        auto span = TSpan::ConstructTerminated(parent, id, start, end, NTraceProto::Status::STATUS_CODE_OK,
            "Terminated", runtime.GetActorSystem(0));
        span.End();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        const auto& terminated = GetSpan(*uploader, "Terminated");
        UNIT_ASSERT_VALUES_EQUAL(terminated.start_time_unix_nano(), start);
        UNIT_ASSERT_VALUES_EQUAL(terminated.end_time_unix_nano(), end);
    }
}
