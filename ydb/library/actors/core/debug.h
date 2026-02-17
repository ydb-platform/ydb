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
    Special,
    Activation,
    Event,
    Trace,
    Test,
};

constexpr EDebugLevel DebugLevel = EDebugLevel::Activation;

template <typename ...TArgs>
inline TString ActorLibDebugMakeMsg(const std::string &file, int line, TArgs&& ...args) {
    TStringBuilder builder;
    builder << "[" << file << ":" << line << "] ";
    ((builder << std::forward<TArgs>(args)), ...);
    builder << Endl;
    return builder;
}

template <typename ...TArgs>
inline TString ActorLibDebugMakeMsgWithSpaces(const std::string &file, int line, TArgs&& ...args) {
    TStringBuilder builder;
    builder << "[" << file << ":" << line << "] ";
    ((builder << std::forward<TArgs>(args) << " "), ...);
    builder << Endl;
    return builder;
}

template <typename ...TArgs>
inline void ActorLibDebugImpl(const std::string &file, int line, TArgs&& ...args) {
    Cerr << ActorLibDebugMakeMsg(file, line, std::forward<TArgs>(args)...);
}

template <typename ...TArgs>
inline void ActorLibDebugWithSpacesImpl(const std::string &file, int line, TArgs&& ...args) {
    Cerr << ActorLibDebugMakeMsgWithSpaces(file, line, std::forward<TArgs>(args)...);
}

#define ACTORLIB_DEBUG(LEVEL, ...) \
    if constexpr (DebugLevel >= LEVEL) { \
        ActorLibDebugImpl(__FILE__, __LINE__, __VA_ARGS__); \
    } do {} while (0) \
// end ACTORLIB_DEBUG

#define ACTORLIB_DEBUG_FROM_EXECUTOR(LEVEL, ...) \
    if constexpr (DebugLevel >= LEVEL) { \
        ActorLibDebugImpl(__FILE__, __LINE__,  __VA_ARGS__); \
    } do {} while (0) \
// end ACTORLIB_DEBUG_FROM_EXECUTOR

#define ACTORLIB_DEBUG_WITH_SPACES(LEVEL, ...) \
    if constexpr (DebugLevel >= LEVEL) { \
        ActorLibDebugWithSpacesImpl(__FILE__, __LINE__, __VA_ARGS__); \
    } do {} while (0) \
// end ACTORLIB_DEBUG_WITH_SPACES

#define ACTORLIB_VERIFY(expr, ...) \
    Y_ABORT_UNLESS((expr), "%s", (ActorLibDebugMakeMsg(__FILE__, __LINE__, __VA_ARGS__).c_str()))

// end ACTORLIB_VERIFY

}
