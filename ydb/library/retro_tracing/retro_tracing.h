#pragma once

#include "retro_span_base.h"

#include <memory>
#include <vector>

namespace NRetro {

struct TSpanCircleBufStats;

template <typename T> requires std::derived_from<T, TRetroSpan>
static void WriteRetroSpan(const T& span) {
    WriteRetroSpanImpl(span.GetType(), reinterpret_cast<const ui8*>(&span));
}

void InitRetroTracing(ui32 numSlots = 100'000);

void WriteRetroSpanImpl(ERetroSpanType type, const ui8* data);

std::vector<TRetroSpan::TPtr> ReadSpansOfTrace(ui64 traceId);

std::vector<std::shared_ptr<TSpanCircleBufStats>> GetBufferStatsSnapshots();

} // namespace NRetro
