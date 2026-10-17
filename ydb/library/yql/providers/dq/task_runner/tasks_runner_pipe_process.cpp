#include "tasks_runner_pipe_process.h"

#include <yql/essentials/utils/log/log.h>

#include <cerrno>
#include <csignal>
#include <utility>

#include <sys/wait.h>

namespace NYql::NTaskRunnerProxy::NPrivate {

TChildProcessState::TChildProcessState()
    : TChildProcessState(::kill, ::waitpid)
{ }

TChildProcessState::TChildProcessState(TKillFunction killFunction, TWaitPidFunction waitPidFunction)
    : KillFunction_(std::move(killFunction))
    , WaitPidFunction_(std::move(waitPidFunction))
{ }

void TChildProcessState::SetPid(int pid)
{
    const std::lock_guard guard(Mutex_);
    Pid_ = pid;
    ExitStatus_ = -1;
}

void TChildProcessState::Kill()
{
    const std::lock_guard guard(Mutex_);
    if (Pid_ > 0) {
        YQL_CLOG(DEBUG, ProviderDq) << "Kill child, pid: " << Pid_;
        KillFunction_(Pid_, SIGKILL);
    }
}

bool TChildProcessState::IsAlive()
{
    const std::lock_guard guard(Mutex_);
    if (Pid_ > 0) {
        Poll(WNOHANG);
    }
    return Pid_ > 0;
}

int TChildProcessState::Wait(TDuration timeout)
{
    const auto deadline = TInstant::Now() + timeout;
    while (true) {
        {
            const std::lock_guard guard(Mutex_);
            if (Pid_ <= 0) {
                return ExitStatus_;
            }
            const int result = Poll(WNOHANG);
            if (Pid_ <= 0) {
                return ExitStatus_;
            }
            if (result < 0 && errno != EINTR) {
                return -1;
            }
            if (TInstant::Now() >= deadline) {
                if (KillFunction_(Pid_, SIGKILL) != 0 && errno != ESRCH) {
                    return -1;
                }
                Poll(/*options*/ 0);
                return ExitStatus_;
            }
        }
        Sleep(TDuration::MilliSeconds(10));
    }
}

int TChildProcessState::Poll(int options)
{
    int status = -1;
    int result;
    do {
        result = WaitPidFunction_(Pid_, &status, options);
    } while (result < 0 && errno == EINTR && options == 0);
    if (result == Pid_) {
        ExitStatus_ = status;
        Pid_ = -1;
    } else if (result < 0 && errno == ECHILD) {
        Pid_ = -1;
    }
    return result;
}

} // namespace NYql::NTaskRunnerProxy::NPrivate
