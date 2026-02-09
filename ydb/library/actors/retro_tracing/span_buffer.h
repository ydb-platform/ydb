#pragma once

#include "retro_span.h"
#include <vector>

namespace NRetroTracing {

void DropThreadLocalBuffer();
void InitializeThreadLocalBuffer();
void WriteSpan(const TRetroSpan* span);
std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTrace(const NWilson::TTraceId& traceId);

} // namespace NRetroTracing
