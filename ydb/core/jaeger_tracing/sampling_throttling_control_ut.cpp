#include "sampling_throttling_configurator.h"
#include "sampling_throttling_control.h"

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJaegerTracing {
namespace {

class TTracingTimeProviderMock : public ITimeProvider {
public:
    explicit TTracingTimeProviderMock(TInstant now)
        : Now_(now)
    {}

    TInstant Now() override {
        return Now_;
    }

    void Advance(TDuration delta) {
        Now_ += delta;
    }

private:
    TInstant Now_;
};

TSettings<double, TWithTag<TThrottlingSettings>> MakeExternalTracingSettings(
        ui8 level, ui64 maxTracesPerMinute, ui64 maxTracesBurst) {
    TSettings<double, TWithTag<TThrottlingSettings>> settings;
    settings.ExternalThrottlingRules[static_cast<size_t>(ERequestType::UNSPECIFIED)].Global.push_back({
        .Level = level,
        .Throttler = {
            .Value = {
                .Level = level,
                .MaxTracesPerMinute = maxTracesPerMinute,
                .MaxTracesBurst = maxTracesBurst,
            },
            .Tag = 1,
        },
    });
    return settings;
}

TString GenerateTraceparent() {
    auto rootTraceId = NWilson::TTraceId::NewTraceId(15, Max<ui32>());
    return rootTraceId.Span(0).ToTraceresponseHeader();
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(SamplingThrottlingControlTests) {
    Y_UNIT_TEST(ExplicitExternalTraceSharesQuotaAndLimitsVerbosity) {
        auto timeProvider = MakeIntrusive<TTracingTimeProviderMock>(TInstant::Now());
        auto randomProvider = CreateDefaultRandomProvider();
        auto configurator = MakeIntrusive<TSamplingThrottlingConfigurator>(timeProvider, randomProvider);
        configurator->UpdateSettings(MakeExternalTracingSettings(9, 60, 0));

        auto uiControl = configurator->GetControl();
        auto grpcControl = configurator->GetControl();
        TRequestDiscriminator discriminator;

        auto uiTrace = uiControl->HandleExternalTracing(discriminator, {}, 15, 7);
        UNIT_ASSERT(uiTrace);
        UNIT_ASSERT_VALUES_EQUAL(uiTrace.GetVerbosity(), 9);
        UNIT_ASSERT_VALUES_EQUAL(uiTrace.GetTimeToLive(), 7);

        auto grpcTrace = grpcControl->HandleTracing(discriminator, GenerateTraceparent());
        UNIT_ASSERT(!grpcTrace); // The UI trace consumed the shared external quota.

        timeProvider->Advance(TDuration::Seconds(1));
        grpcTrace = grpcControl->HandleTracing(discriminator, GenerateTraceparent());
        UNIT_ASSERT(grpcTrace);
        UNIT_ASSERT_VALUES_EQUAL(grpcTrace.GetVerbosity(), 9);

        auto throttledUiTrace = uiControl->HandleExternalTracing(discriminator, {}, 15, 7);
        UNIT_ASSERT(!throttledUiTrace); // The gRPC trace consumed the same quota.

        timeProvider->Advance(TDuration::Seconds(1));
        auto lowVerbosityUiTrace = uiControl->HandleExternalTracing(discriminator, {}, 4, 7);
        UNIT_ASSERT(lowVerbosityUiTrace);
        UNIT_ASSERT_VALUES_EQUAL(lowVerbosityUiTrace.GetVerbosity(), 4);
    }

    Y_UNIT_TEST(ExplicitExternalTraceRequiresExternalThrottlingRule) {
        auto timeProvider = MakeIntrusive<TTracingTimeProviderMock>(TInstant::Now());
        auto randomProvider = CreateDefaultRandomProvider();
        auto configurator = MakeIntrusive<TSamplingThrottlingConfigurator>(timeProvider, randomProvider);
        auto control = configurator->GetControl();

        UNIT_ASSERT(!control->HandleExternalTracing({}, {}, 15, Max<ui32>()));
    }
}

} // namespace NKikimr::NJaegerTracing
