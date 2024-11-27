#include "wilson_tracing_control.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>

#include <util/system/yassert.h>

namespace NKikimr::NJaegerTracing {

namespace {

TIntrusivePtr<TSamplingThrottlingControl> CreateNewTracingControl() {
    Y_ASSERT(HasAppData()); // In general we must call this from actor thread
    if (!HasAppData()) {
        return nullptr;
    }

    return AppData()->TracingConfigurator->GetControl();
}

TSamplingThrottlingControl* GetTracingControlTls() {
    static thread_local TIntrusivePtr<TSamplingThrottlingControl> Control = CreateNewTracingControl();
    return Control.Get();
}

} // namespace

void HandleTracing(NWilson::TTraceId& traceId, const TRequestDiscriminator& discriminator) {
    if (TSamplingThrottlingControl* control = GetTracingControlTls()) {
        control->HandleTracing(traceId, discriminator);
    }
}

} // namespace NKikimr::NJaegerTracing
