#include "trace_service_gate.h"

#include "trace_service.h"

#include <ydb/library/actors/wilson/wilson_span.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Records every CreateRootSpan call. Always returns an empty (disabled) span
// so no span data has to be ended (an unfinished enabled span aborts on
// destruction in debug builds).
class TRecordingTraceService final: public ITraceService
{
public:
    ui32 CallCount = 0;
    TVector<TString> Names;

    NWilson::TSpan CreateRootSpan(TStringBuf name) override
    {
        ++CallCount;
        Names.emplace_back(name);
        return NWilson::TSpan();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTraceServiceGateTest)
{
    Y_UNIT_TEST(ShouldForwardCreateRootSpanToAttachedService)
    {
        auto service = std::make_shared<TRecordingTraceService>();
        TTraceServiceGate gate(service);

        auto span1 = gate.CreateRootSpan("span-1");
        auto span2 = gate.CreateRootSpan("span-2");

        UNIT_ASSERT_VALUES_EQUAL(2, service->CallCount);
        UNIT_ASSERT_VALUES_EQUAL(2, service->Names.size());
        UNIT_ASSERT_VALUES_EQUAL("span-1", service->Names[0]);
        UNIT_ASSERT_VALUES_EQUAL("span-2", service->Names[1]);
    }

    Y_UNIT_TEST(ShouldReturnEmptySpanWhenDetached)
    {
        auto service = std::make_shared<TRecordingTraceService>();
        TTraceServiceGate gate(service);

        gate.Detach();

        auto span = gate.CreateRootSpan("span");

        // A detached gate must not touch the underlying service and must
        // return an empty (disabled) span.
        UNIT_ASSERT_VALUES_EQUAL(0, service->CallCount);
        UNIT_ASSERT(!span);
    }

    Y_UNIT_TEST(ShouldForwardAgainAfterReattach)
    {
        auto service = std::make_shared<TRecordingTraceService>();
        TTraceServiceGate gate(service);

        gate.Detach();
        {
            auto span = gate.CreateRootSpan("ignored");
            UNIT_ASSERT(!span);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, service->CallCount);

        gate.Attach(service);
        auto span = gate.CreateRootSpan("after-reattach");

        UNIT_ASSERT_VALUES_EQUAL(1, service->CallCount);
        UNIT_ASSERT_VALUES_EQUAL("after-reattach", service->Names.back());
    }

    Y_UNIT_TEST(ShouldSwitchToNewServiceOnAttach)
    {
        auto firstService = std::make_shared<TRecordingTraceService>();
        auto secondService = std::make_shared<TRecordingTraceService>();

        TTraceServiceGate gate(firstService);

        auto span1 = gate.CreateRootSpan("first");
        UNIT_ASSERT_VALUES_EQUAL(1, firstService->CallCount);
        UNIT_ASSERT_VALUES_EQUAL(0, secondService->CallCount);

        gate.Attach(secondService);

        auto span2 = gate.CreateRootSpan("second");
        UNIT_ASSERT_VALUES_EQUAL(1, firstService->CallCount);
        UNIT_ASSERT_VALUES_EQUAL(1, secondService->CallCount);
        UNIT_ASSERT_VALUES_EQUAL("second", secondService->Names.back());
    }
}

}   // namespace NYdb::NBS::NBlockStore
