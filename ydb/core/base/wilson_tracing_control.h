#pragma once
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

// Generate a new trace id (or throttle existing one)
// with probability according to current configuration and request type.
// Can be called from actor system threads.
void HandleTracing(NWilson::TTraceId& traceId, const TRequestDiscriminator& discriminator);

// For test purposes
// Clears tracing control TLS variables that depend on AppData
void ClearTracingControl();

} // namespace NKikimr::NJaegerTracing
