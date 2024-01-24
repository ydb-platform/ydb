#include "tracing_control.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/protos/config.pb.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr {

namespace {

const NKikimrConfig::TImmediateControlOptions& GetImmediateControlOptionsForField(
        const google::protobuf::Descriptor& descriptor, TString fieldName) {
    auto field = descriptor.FindFieldByName(fieldName);
    Y_ABORT_UNLESS(field);
    auto& fieldOptions = field->options();
    return fieldOptions.GetExtension(NKikimrConfig::ControlOptions);
}

TThrottler CreateThrottler(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider, TString domain) {
    TControlWrapper maxRatePerMinute;
    TControlWrapper maxBurst;

    const std::array<std::pair<TControlWrapper&, TStringBuf>, 2> controls = {{
        {maxRatePerMinute, "MaxRatePerMinute"},
        {maxBurst, "MaxBurst"},
    }};
    const auto& throttlingOptions = *NKikimrConfig::TImmediateControlsConfig::TTracingControls::TSamplingThrottlingOptions::TThrottlingOptions::descriptor();
    for (auto& [control, fieldName] : controls) {
        const auto& controlOptions = GetImmediateControlOptionsForField(throttlingOptions, TString(fieldName));

        control.Reset(controlOptions.GetDefaultValue(), controlOptions.GetMinValue(), controlOptions.GetMaxValue());
        icb->RegisterSharedControl(control, domain + "." + fieldName);
    }

    return TThrottler(std::move(maxRatePerMinute), std::move(maxBurst), std::move(timeProvider));
}

}

TTracingControl::TTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider>& randomProvider, TString controlDomain)
{
    SampledThrottler = CreateThrottler(icb, timeProvider, controlDomain + ".SampledThrottling");
    ExternalThrottler = CreateThrottler(icb, timeProvider, controlDomain + ".ExternalThrottling");

    TControlWrapper samplingPPM;
    const std::array<std::pair<TControlWrapper&, TStringBuf>, 2> controls = {{
        {samplingPPM, "PPM"},
        {SampledLevel, "Level"},
    }};

    const auto& samplingOptions = *NKikimrConfig::TImmediateControlsConfig::TTracingControls::TSamplingThrottlingOptions::TSamplingOptions::descriptor();
    for (auto [control, name] : controls) {
        const auto& controlOptions = GetImmediateControlOptionsForField(samplingOptions, TString(name));
        control.Reset(controlOptions.GetDefaultValue(), controlOptions.GetMinValue(), controlOptions.GetMaxValue());
        icb->RegisterSharedControl(control, controlDomain + ".Sampling." + name);
    }

    Sampler = TSampler(std::move(samplingPPM), randomProvider->GenRand64());
}

} // namespace NKikimr
