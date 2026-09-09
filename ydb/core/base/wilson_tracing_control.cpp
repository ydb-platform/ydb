#include "wilson_tracing_control.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>

#include <util/thread/singleton.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

namespace NKikimr::NJaegerTracing {

namespace {

class TSamplingThrottlingControlTlsHolder {
public:
    TSamplingThrottlingControl* Get() {
        if (Y_UNLIKELY(!Control)) {
            Control = CreateControl();
        }
        return Control.Get();
    }

    void Reset() {
        Control = nullptr;
    }

private:
    static TIntrusivePtr<TSamplingThrottlingControl> CreateControl() {
        Y_ASSERT(HasAppData()); // In general we must call this from actor thread
        if (Y_UNLIKELY(!HasAppData())) {
            return nullptr;
        }

        return AppData()->TracingConfigurator->GetControl();
    }

private:
    TIntrusivePtr<TSamplingThrottlingControl> Control;
};

} // namespace

NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent) {
    TSamplingThrottlingControl* control = FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->Get();
    if (Y_LIKELY(control)) {
        return control->HandleTracing(discriminator, traceparent);
    }
    return NWilson::TTraceId{};
}

void ClearTracingControl() {
    FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->Reset();
}

} // namespace NKikimr::NJaegerTracing
