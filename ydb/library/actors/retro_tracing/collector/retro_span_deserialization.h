#pragma once

#include <ydb/library/actors/retro_tracing/span/retro_span.h>

#include <memory>
#include <vector>

namespace NRetroTracing {

// User of the library must provide the definition of this method
// See UT for implementation example
TRetroSpan* DeserializeRetroSpanImpl(ui32 type, ui32 size, const void* data);

std::unique_ptr<TRetroSpan> DeserializeRetroSpanToUnique(const void* data);

// helper functions
std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTrace(const NWilson::TTraceId& traceId);
std::vector<std::unique_ptr<TRetroSpan>> GetSpansOfTraces(const std::vector<NWilson::TTraceId>& traceIds);
std::vector<std::unique_ptr<TRetroSpan>> GetAllSpans();

};
