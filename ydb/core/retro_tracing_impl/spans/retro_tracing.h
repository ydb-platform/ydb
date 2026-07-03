#pragma once

#include <ydb/library/actors/retro_tracing/span/retro_span.h>
#include <ydb/library/actors/retro_tracing/span/retro_span_namespace.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr {

enum ERetroSpanType : ui32 {
    NamedSpan = NRetroTracing::TSpanTypeNamespace::Begin(NRetroTracing::TSpanTypeNamespace::USERSPACE),
    End
};

static_assert(End < NRetroTracing::TSpanTypeNamespace::End(NRetroTracing::TSpanTypeNamespace::USERSPACE));

} // namespace NKikimr
