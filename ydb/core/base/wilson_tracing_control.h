#pragma once
#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

// Generate a new trace id (or throttle existing one)
// with probability according to current configuration and request type.
// Can be called from actor system threads.
NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator, const TMaybe<TString>& traceparent);

// Start or continue a trace explicitly requested by an external caller.
// The trace is admitted by external throttling rules and its verbosity is
// limited by both the caller and the matching configuration.
NWilson::TTraceId HandleExternalTracing(const TRequestDiscriminator& discriminator,
        const TMaybe<TString>& traceparent, ui8 maxVerbosity, ui32 timeToLive);

// For test purposes
// Clears tracing control TLS variables that depend on AppData
void ClearTracingControl();

} // namespace NKikimr::NJaegerTracing
