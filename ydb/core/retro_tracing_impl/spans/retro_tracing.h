#pragma once

#include <ydb/library/actors/retro_tracing/retro_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr {

enum ERetroSpanType : ui32 {
    NamedSpan = 1,
};

} // namespace NKikimr
