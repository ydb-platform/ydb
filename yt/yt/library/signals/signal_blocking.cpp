#include "signal_blocking.h"

#include <yt/yt/library/procfs/procfs.h>

#include <signal.h>

namespace NYT::NSignals {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void TryVerifyThreadIsOnly()
{
#ifdef _linux_
    int threadCount = 0;
    try {
        threadCount = NProcFS::GetThreadCount();
    } catch (const std::exception& ex) {
        Cerr << "Failed to get thread count, ex: " << ex.what() << Endl;
        ::exit(static_cast<int>(EErrorCode::SetBlockedSignalError));
    } catch (...) {
        Cerr << "Failed to get thread count, unknown exception" << Endl;
        ::exit(static_cast<int>(EErrorCode::SetBlockedSignalError));
    }

    if (threadCount != 1) {
        Cerr << "Thread count is not 1, threadCount: " << threadCount << Endl;
        ::exit(static_cast<int>(EErrorCode::SetBlockedSignalError));
    }
#endif // _linux_
}

} // namespace NDetail

void BlockSignal(int signal)
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
