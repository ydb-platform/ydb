#include "process_exit_profiler.h"

#include <algorithm>
#include <cctype>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TProcessExitProfiler::TProcessExitProfiler(
    const NProfiling::TProfiler& parent,
    const std::string& prefix)
    : Profiler_(parent.WithPrefix(prefix))
    , ExitDelayTimer_(Profiler_.Timer("/exit_delay"))
    , ExitOKCounter_(Profiler_.Counter("/zero_exit_code"))
    , ExitUnknownCounter_(Profiler_.Counter("/unknown"))
{ }

void TProcessExitProfiler::OnProcessExit(
    const TError& error,
    std::optional<TDuration> delay)
{
    if (delay) {
        ExitDelayTimer_.Record(*delay);
    }

    if (error.IsOK()) {
        ExitOKCounter_.Increment();
        return;
    }

    if (error.FindMatching(EProcessErrorCode::NonZeroExitCode)) {
        if (auto exitCode = FindAttributeRecursive<int>(error, "exit_code")) {
            GetOrCreateNonZeroExitCounter(*exitCode).Increment();
            return;
        }
    }

    if (error.FindMatching(EProcessErrorCode::Signal)) {
        if (auto signal = FindAttributeRecursive<int>(error, "signal")) {
            GetOrCreateSignalExitCounter(*signal).Increment();
            return;
        }
    }

    ExitUnknownCounter_.Increment();
}

NProfiling::TCounter& TProcessExitProfiler::GetOrCreateNonZeroExitCounter(int exitCode)
{
    auto iter = NonZeroExitCounters_.find(exitCode);
    if (iter != NonZeroExitCounters_.end()) {
        return iter->second;
    }

    return NonZeroExitCounters_.emplace(exitCode, MakeExitCodeCounter(exitCode)).first->second;
}

NProfiling::TCounter TProcessExitProfiler::MakeExitCodeCounter(int exitCode)
{
    return Profiler_
        .WithTag("non_zero_exit_code", ToString(exitCode))
        .Counter("/count");
}

NProfiling::TCounter& TProcessExitProfiler::GetOrCreateSignalExitCounter(int signal)
{
    auto iter = SignalExitCounters_.find(signal);
    if (iter != SignalExitCounters_.end()) {
        return iter->second;
    }

    return SignalExitCounters_.emplace(signal, MakeSignalExitCounter(signal)).first->second;
}

NProfiling::TCounter TProcessExitProfiler::MakeSignalExitCounter(int signal)
{
    return Profiler_
        .WithTag("terminated_by_signal", GetSignalName(signal))
        .Counter("/count");
}

std::string TProcessExitProfiler::GetSignalName(int signal)
{
#ifdef _unix_
    auto result = std::string(strsignal(signal));
    std::transform(result.begin(), result.end(), result.begin(), [] (unsigned char c) {
        return c == ' '
            ? '_'
            : std::tolower(c);
    });

    return result;
#else
    return ToString(signal);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
