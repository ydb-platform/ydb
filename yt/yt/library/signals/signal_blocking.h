#pragma once

#include <library/cpp/yt/error/error.h>

#include <util/system/platform.h>

#include <limits>

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void TryVerifyThreadIsOnly();

} // namespace NDetail

void BlockSignal(int signal);

YT_DEFINE_ERROR_ENUM(
    ((SetBlockedSignalError)     (20))
);

// Using constructor(std::numeric_limits<i32>::min()) to run before any thread can be spawned.
#define YT_BLOCK_SIGNAL_FOR_PROCESS(signal) \
    __attribute__((constructor(std::numeric_limits<i32>::min()))) void Block ## signal ## Signal() \
    { \
        try { \
            NDetail::TryVerifyThreadIsOnly(); \
            NSignals::BlockSignal(signal); \
        } catch (const std::exception& ex) { \
            Cerr << "Failed to block signal " << #signal << ": " << ex.what() << Endl; \
            ::exit(static_cast<int>(EErrorCode::SetBlockedSignalError)); \
        } catch (...) { \
            Cerr << "Failed to block signal " << #signal << ": unknown exception" << Endl; \
            ::exit(static_cast<int>(EErrorCode::SetBlockedSignalError)); \
        } \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals
