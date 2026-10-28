#include "tasks_runner_pipe_process.h"

#include <utility>

namespace NYql::NTaskRunnerProxy::NPrivate {

TChildProcessState::TChildProcessState() = default;

TChildProcessState::TChildProcessState(TKillFunction killFunction, TWaitPidFunction waitPidFunction)
    : KillFunction_(std::move(killFunction))
    , WaitPidFunction_(std::move(waitPidFunction))
{ }

void TChildProcessState::SetPid(int /*pid*/)
{ }

void TChildProcessState::Kill()
{ }

bool TChildProcessState::IsAlive()
{
    return true;
}

int TChildProcessState::Wait(TDuration /*timeout*/)
{
    return -1;
}

} // namespace NYql::NTaskRunnerProxy::NPrivate
