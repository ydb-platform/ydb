#include "wilson_tracing_control.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>

#include <util/thread/singleton.h>
#include <util/system/compiler.h>

#include <array>
#include <util/system/tls.h>
#include <util/system/yassert.h>

namespace NKikimr::NJaegerTracing {

namespace {

// One control per tracing channel; indexed into the arrays below.
enum class ETracingChannel : size_t {
    Dev = 0,
    User = 1,
    Count = 2,
};

TIntrusivePtr<TSamplingThrottlingConfigurator> ChannelConfigurator(ETracingChannel channel) {
    return channel == ETracingChannel::User
        ? AppData()->UserFacingTracingConfigurator
        : AppData()->TracingConfigurator;
}

// Per-thread cache of each channel's control. The holder itself is thread-local via
// FastTlsSingleton, so the array needs no extra thread-local machinery.
class TSamplingThrottlingControlTlsHolder {
public:
    TSamplingThrottlingControl* Get(ETracingChannel channel) {
        auto& control = Controls[static_cast<size_t>(channel)];
        if (Y_UNLIKELY(!control)) {
            control = CreateControl(channel);
        }
        return control.Get();
    }

    void Reset(ETracingChannel channel) {
        Controls[static_cast<size_t>(channel)] = nullptr;
    }

private:
    static TIntrusivePtr<TSamplingThrottlingControl> CreateControl(ETracingChannel channel) {
        Y_ASSERT(HasAppData()); // In general we must call this from an actor thread
        if (Y_UNLIKELY(!HasAppData())) {
            return nullptr;
        }
        return ChannelConfigurator(channel)->GetControl();
    }

    std::array<TIntrusivePtr<TSamplingThrottlingControl>, static_cast<size_t>(ETracingChannel::Count)> Controls;
};

NWilson::TTraceId HandleTracing(ETracingChannel channel, const TRequestDiscriminator& discriminator,
        const TMaybe<TString>& traceparent) {
    TSamplingThrottlingControl* control = FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->Get(channel);
    if (Y_LIKELY(control)) {
        return control->HandleTracing(discriminator, traceparent);
    }
    return NWilson::TTraceId{};
}

void ClearTracingControl(ETracingChannel channel) {
    FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->Reset(channel);
}

} // namespace

NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent) {
    return HandleTracing(ETracingChannel::Dev, discriminator, traceparent);
}

NWilson::TTraceId HandleUserFacingTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent) {
    return HandleTracing(ETracingChannel::User, discriminator, traceparent);
}

void ClearTracingControl() {
    ClearTracingControl(ETracingChannel::Dev);
}

void ClearUserFacingTracingControl() {
    ClearTracingControl(ETracingChannel::User);
}

} // namespace NKikimr::NJaegerTracing
