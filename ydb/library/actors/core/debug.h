#pragma once

#include <ydb/library/actors/core/probes.h>

namespace NActors {

enum class EDebugLevel {
    None = 0,
    ActorSystem,
    ExecutorPool,
    Harmonizer,
    Lease,
    Executor,
    Activation,
    Event,
    Trace,
    Test,
};

constexpr EDebugLevel DebugLevel = EDebugLevel::ActorSystem;

template <typename ...TArgs>
inline void ActorLibDebugImpl(const std::string &file, int line, TArgs&& ...args) {
    TStringBuilder builder;
    builder << "[" << file << ":" << line << "] ";
    ((builder << std::forward<TArgs>(args)), ...);
    builder << Endl;
    Cerr << builder;
}

template <typename ...TArgs>
inline void ActorLibDebugWithSpacesImpl(const std::string &file, int line, TArgs&& ...args) {
    TStringBuilder builder;
    builder << "[" << file << ":" << line << "] ";
    ((builder << std::forward<TArgs>(args) << " "), ...);
    builder << Endl;
    Cerr << builder;
}

#define ACTORLIB_DEBUG(LEVEL, ...) \
    if constexpr (DebugLevel >= LEVEL) { \
        ActorLibDebugImpl(__FILE__, __LINE__, __VA_ARGS__); \
    } do {} while (0) \
// end ACTORLIB_DEBUG

#define ACTORLIB_DEBUG_WITH_SPACES(LEVEL, ...) \
    if constexpr (DebugLevel >= LEVEL) { \
        ActorLibDebugWithSpacesImpl(__FILE__, __LINE__, __VA_ARGS__); \
    } do {} while (0) \
// end ACTORLIB_DEBUG_WITH_SPACES

}
