#pragma once

#include "retro_span.h"
#include <array>
#include <functional>

namespace NRetroTracing {

using TBufferData = std::array<char, BufferSize>;

void DropThreadLocalBuffer();
void InitializeThreadLocalBuffer();
void WriteSpan(const TRetroSpan* span);

void AccessBuffers(TBufferData* readBuffer, std::function<void()> callback);

} // namespace NRetroTracing
