#pragma once

#include <library/cpp/yt/error/error.h>

#include <library/cpp/yt/misc/concepts.h>

#include <library/cpp/yt/misc/static_initializer.h>

#include <util/system/platform.h>

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

class TInitHelper
{
public:
    template <CInvocable<void()> TFunc>
    TInitHelper(const TFunc& func)
    {
        func();
    }
};

void TryVerifyThreadIsOnly();

template <CInvocable<void(bool ok, int threadCount)> TFunc>
void BlockSignalAtProcessStart(int signal, const TFunc& func);

} // namespace NDetail

void BlockSignal(int signal);

YT_DEFINE_ERROR_ENUM(
    ((SetBlockedSignalError)     (20))
);

// NB(pogorelov): Using init_priority(101) to run before any thread can be spawned.
// And also therefore this stuff was intentionally not moved to YT_STATIC_INITIALIZER.
#define YT_TRY_BLOCK_SIGNAL_FOR_PROCESS(signal, ...) \
    [[maybe_unused]] __attribute__((init_priority(101))) inline const ::NYT::NSignals::NDetail::TInitHelper Signal ## Blocked([] { \
        NSignals::NDetail::BlockSignalAtProcessStart(signal, __VA_ARGS__); \
    })

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals

#define SIGNAL_BLOCKING_INL_H_
#include "signal_blocking-inl.h"
#undef SIGNAL_BLOCKING_INL_H_
