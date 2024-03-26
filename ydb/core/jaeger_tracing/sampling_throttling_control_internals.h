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
    TSamplingThrottlingImpl(TSettings<TSampler, TIntrusivePtr<TThrottler>>&& settings)
        : Setup(std::move(settings))
    {}

    TSettings<TSampler, TIntrusivePtr<TThrottler>> Setup;

    void HandleTracing(NWilson::TTraceId& traceId, TRequestDiscriminator discriminator);
};

} // namespace NKikimr::NJaegerTracing
