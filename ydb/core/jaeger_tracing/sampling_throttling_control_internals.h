#pragma once

#include "sampler.h"
#include "throttler.h"
#include "sampling_throttling_control.h"
#include "settings.h"

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr::NJaegerTracing {

struct TSamplingThrottlingControl::TSamplingThrottlingImpl {
    TSettings<TSampler, TIntrusivePtr<TThrottler>> Setup;

    void HandleTracing(NWilson::TTraceId& traceId, const TRequestDiscriminator& discriminator);

private:
    bool Throttle(ERequestType requestType);

    TMaybe<ui8> Sample(ERequestType requestType);
};

} // namespace NKikimr::NJaegerTracing
