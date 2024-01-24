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

}

TTracingControl::TTracingControl(TIntrusivePtr<TControlBoard>& icb, TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider>& randomProvider, TString controlDomain)
{
    Y_UNUSED(icb, controlDomain, GetImmediateControlOptionsForField);
    TControlWrapper samplingPPM;
    TControlWrapper maxRatePerMinute;
    TControlWrapper maxBurst;

    // const std::array<std::tuple<TControlWrapper&, TStringBuf>, 3> kek = {{
    //     {samplingPPM, "SamplingPPM"},
    //     {maxRatePerMinute, "MaxRatePerMinute"},
    //     {maxBurst, "MaxBurst"},
    // }};
    //
    // for (auto [control, name] : kek) {
    //     auto& controlOptions = GetImmediateControlOptionsForField(*NKikimrConfig::TImmediateControlsConfig::TTracingControls::TSamplingThrottlingControls::descriptor(), TString(name));
    //     control.Reset(controlOptions.GetDefaultValue(), controlOptions.GetMinValue(), controlOptions.GetMaxValue());
    //     icb->RegisterSharedControl(control, controlDomain + "." + name);
    // }

    Sampler = TSampler(std::move(samplingPPM), randomProvider->GenRand64());
    Throttler = TThrottler(std::move(maxRatePerMinute), std::move(maxBurst), std::move(timeProvider));
}

} // namespace NKikimr
