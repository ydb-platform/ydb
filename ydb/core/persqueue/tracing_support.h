#pragma once

#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <util/generic/fwd.h>

namespace NKikimr::NPQ {

NWilson::TSpan GenerateSpan(const TStringBuf name, NJaegerTracing::TSamplingThrottlingControl& tracingControl);

}
