#pragma once

#include <ydb/library/actors/core/probes.h>

namespace NActors {

enum class EDebugHarmonizerLevel {
    None = 0,
    Debug = 1,
    Trace = 2,
    History = 3,
};

#ifdef HARMONIZER_DEBUG
constexpr EDebugHarmonizerLevel DebugHarmonizerLevel = EDebugHarmonizerLevel::Debug;
#elif defined(HARMONIZER_HISTORY_DEBUG)
constexpr EDebugHarmonizerLevel DebugHarmonizerLevel = EDebugHarmonizerLevel::History;
#else
constexpr EDebugHarmonizerLevel DebugHarmonizerLevel = EDebugHarmonizerLevel::None;
#endif

template <typename ... TArgs>
void Print(TArgs&& ... args) {
    ((Cerr << std::forward<TArgs>(args) << " "), ...) << Endl;
}

#define HARMONIZER_DEBUG_PRINT(...) \
    if constexpr (DebugHarmonizerLevel >= EDebugHarmonizerLevel::Debug) { Print(__VA_ARGS__); }
// HARMONIZER_DEBUG_PRINT(...)

#define LWPROBE_WITH_DEBUG(probe, ...) \
    LWPROBE(probe, __VA_ARGS__); \
    HARMONIZER_DEBUG_PRINT(#probe, __VA_ARGS__); \
// LWPROBE_WITH_DEBUG(probe, ...)

#define HARMONIZER_TRACE_PRINT(...) \
    if constexpr (DebugHarmonizerLevel >= EDebugHarmonizerLevel::Trace) { Print(__VA_ARGS__); }
// HARMONIZER_TRACE_PRINT(...)

#define LWPROBE_WITH_TRACE(probe, ...) \
    LWPROBE(probe, __VA_ARGS__); \
    HARMONIZER_TRACE_PRINT(#probe, __VA_ARGS__); \
// LWPROBE_WITH_TRACE(probe, ...)

#define HARMONIZER_HISTORY_PRINT(...) \
    if constexpr (DebugHarmonizerLevel >= EDebugHarmonizerLevel::History) { Print(__VA_ARGS__); }
// HARMONIZER_HISTORY_PRINT(...)

#define LWPROBE_WITH_HISTORY(probe, ...) \
    LWPROBE(probe, __VA_ARGS__); \
    HARMONIZER_HISTORY_PRINT(#probe, __VA_ARGS__); \
// LWPROBE_WITH_HISTORY(probe, ...)

} // namespace NActors
