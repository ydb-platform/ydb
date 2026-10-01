#include "wilson_tracing_control.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>

#include <util/thread/singleton.h>
#include <util/system/compiler.h>
#include <util/system/tls.h>
#include <util/system/yassert.h>

namespace NKikimr::NJaegerTracing {

namespace {

Y_POD_STATIC_THREAD(TSamplingThrottlingControl*) TracingControlRawPtr;

class TSamplingThrottlingControlTlsHolder {
public:
    TSamplingThrottlingControlTlsHolder()
        : Control(CreateNewTracingControl())
    {}

    TSamplingThrottlingControl* GetTracingControlPtr() {
        if (Y_UNLIKELY(!Control)) {
            Control = CreateNewTracingControl();
        }
        return Control.Get();
    }

    void ResetTracingControl() {
        Control = nullptr;
    }

private:
    static TIntrusivePtr<TSamplingThrottlingControl> CreateNewTracingControl() {
        Y_ASSERT(HasAppData()); // In general we must call this from actor thread
        if (Y_UNLIKELY(!HasAppData())) {
            return nullptr;
        }

        return AppData()->TracingConfigurator->GetControl();
    }

private:
    TIntrusivePtr<TSamplingThrottlingControl> Control;
};

TSamplingThrottlingControl* GetTracingControlTls() {
    if (Y_UNLIKELY(!TracingControlRawPtr)) {
        TracingControlRawPtr = FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->GetTracingControlPtr();
    }
    return TracingControlRawPtr;
}

} // namespace

NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent) {
    TSamplingThrottlingControl* control = GetTracingControlTls();
    if (Y_LIKELY(control)) {
        return control->HandleTracing(discriminator, traceparent);
    }
    return NWilson::TTraceId{};
}

void ClearTracingControl() {
    if (TracingControlRawPtr) {
        TracingControlRawPtr = nullptr;
        FastTlsSingleton<TSamplingThrottlingControlTlsHolder>()->ResetTracingControl();
    }
}

} // namespace NKikimr::NJaegerTracing
