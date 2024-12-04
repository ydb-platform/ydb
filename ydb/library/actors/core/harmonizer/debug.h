#pragma once

#include <ydb/library/actors/core/probes.h>

namespace NActors {

constexpr bool DebugHarmonizer = false;

template <typename ... TArgs>
void Print(TArgs&& ... args) {
    ((Cerr << std::forward<TArgs>(args) << " "), ...) << Endl;
}

#define HARMONIZER_DEBUG_PRINT(...) \
    if constexpr (DebugHarmonizer) { Print(__VA_ARGS__); }
// HARMONIZER_DEBUG_PRINT(...)

#define LWPROBE_WITH_DEBUG(probe, ...) \
    LWPROBE(probe, __VA_ARGS__); \
    HARMONIZER_DEBUG_PRINT(#probe, __VA_ARGS__); \
// LWPROBE_WITH_DEBUG(probe, ...)

} // namespace NActors
