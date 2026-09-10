#include "tasks_runner_pipe_process.h"

#include <util/system/thread.h>

#include <cerrno>
#include <csignal>

#ifndef _win_
#include <sys/wait.h>
#endif

namespace NYql::NTaskRunnerProxy::NPrivate {

bool CleanupChildProcess(
    int* pid,
    TDuration timeout,
    const TKillFunction& killFunction,
    const TWaitPidFunction& waitPidFunction)
{
#ifdef _win_
    Y_UNUSED(pid);
    Y_UNUSED(timeout);
    Y_UNUSED(killFunction);
    Y_UNUSED(waitPidFunction);
    return true;
#else
    if (*pid <= 0) {
        return true;
    }

    const int childPid = *pid;
    if (killFunction(childPid, SIGKILL) != 0 && errno != ESRCH) {
        return false;
    }

    int status = 0;
    const auto deadline = TInstant::Now() + timeout;
    while (true) {
        const int result = waitPidFunction(childPid, &status, WNOHANG);
        if (result == childPid) {
            *pid = -1;
            return true;
        }
        if (result < 0) {
            if (errno == EINTR) {
                if (TInstant::Now() >= deadline) {
                    return false;
                }
                continue;
            }
            if (errno == ECHILD) {
                *pid = -1;
                return true;
            }
            return false;
        }
        if (TInstant::Now() >= deadline) {
            return false;
        }
        Sleep(TDuration::MilliSeconds(10));
    }
#endif
}

bool IsShellCommandSuccessful(
    TShellCommand::ECommandStatus status,
    const TMaybe<int>& exitCode)
{
    return status == TShellCommand::SHELL_FINISHED && exitCode && *exitCode == 0;
}

} // namespace NYql::NTaskRunnerProxy::NPrivate
