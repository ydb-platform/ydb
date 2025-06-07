#ifndef SIGNAL_BLOCKING_INL_H_
#error "Direct inclusion of this file is not allowed, include signal_blocking.h"
// For the sake of sane code completion.
#include "signal_blocking.h"
#endif

#include "signal_blocking.h"

#include <yt/yt/library/procfs/procfs.h>

#include <library/cpp/yt/system/exit.h>

#include <signal.h>

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <CInvocable<void(bool ok, int threadCount)> TFunc>
bool ValidateSingleRunningThread(const TFunc& func)
{
#ifdef _linux_
    int threadCount = 0;
    try {
        threadCount = NProcFS::GetThreadCount();
    } catch (const std::exception& ex) {
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format("Failed to get thread count: %v", ex.what()));
    } catch (...) {
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            "Failed to get thread count: unknown exception");
    }

    bool ok = threadCount == 1;
    func(ok, threadCount);
    return ok;
#endif // _linux_
}

template <CInvocable<void(bool ok, int threadCount)> TFunc>
void BlockSignalAtProcessStart(int signal, const TFunc& func)
{
    try {
        if (::NYT::NSignals::NDetail::ValidateSingleRunningThread(func)) {
            ::NYT::NSignals::BlockSignal(signal);
        }
    } catch (const std::exception& ex) {
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format("Failed to block signal %v: %v", signal, ex.what()));
    } catch (...) {
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format("Failed to block signal %v: unknown exception", signal));
    }
}

} // namespace NDetail

inline void BlockSignal(int signal)
{
#ifndef _unix_
    THROW_ERROR_EXCEPTION("Signal blocking is not supported on this platform");
#else // _unix_

    sigset_t mask;
    if (sigprocmask(SIG_BLOCK, nullptr, &mask) == -1) {
        THROW_ERROR_EXCEPTION(EErrorCode::SetBlockedSignalError, "Failed to get blocked signal mask while blocking signal")
            << TErrorAttribute("signal_to_block", signal);
    }
    if (sigaddset(&mask, signal) == -1) {
        THROW_ERROR_EXCEPTION(EErrorCode::SetBlockedSignalError, "Failed to add signal to mask while blocking signal")
            << TErrorAttribute("signal_to_block", signal);
    }

    if (sigprocmask(SIG_BLOCK, &mask, nullptr) == -1) {
        THROW_ERROR_EXCEPTION(EErrorCode::SetBlockedSignalError, "Failed to set blocked signal mask while blocking signal")
            << TErrorAttribute("signal_to_block", signal);
    }
#endif // _unix_
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignals
