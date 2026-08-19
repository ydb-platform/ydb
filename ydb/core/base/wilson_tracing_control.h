#pragma once
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

// Generate a new trace id (or throttle existing one)
// with probability according to current configuration and request type.
// Can be called from actor system threads.
NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent);

NWilson::TTraceId HandleUserFacingTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent);

// For test purposes
void ClearTracingControl();

} // namespace NKikimr::NJaegerTracing
