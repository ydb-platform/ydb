#pragma once

#include "retro_span.h"
#include <array>
#include <functional>

namespace NRetroTracing {

inline constexpr ui32 SpanCellSize = 1 << 10;
inline constexpr ui32 SpanBufferSize = 4 << 20;

using TBufferData = std::array<char, SpanBufferSize>;

void DropThreadLocalBuffer();
void InitializeThreadLocalBuffer();
void WriteSpan(const TRetroSpan* span);

void AccessBuffers(TBufferData* readBuffer, std::function<void()> callback);

} // namespace NRetroTracing
