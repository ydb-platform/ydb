#pragma once

#include <util/datetime/base.h>

#include <functional>
#include <mutex>

namespace NYql::NTaskRunnerProxy::NPrivate {

using TKillFunction = std::function<int(int, int)>;
using TWaitPidFunction = std::function<int(int, int*, int)>;

class TChildProcessState
{
public:
    TChildProcessState();
    TChildProcessState(TKillFunction killFunction, TWaitPidFunction waitPidFunction);

    void SetPid(int pid);
    void Kill();
    bool IsAlive();
    int Wait(TDuration timeout = TDuration::Seconds(5));

private:
    const TKillFunction KillFunction_;
    const TWaitPidFunction WaitPidFunction_;

    std::mutex Mutex_;
    int Pid_ = -1;
    int ExitStatus_ = -1;

    int Poll(int options);
};

} // namespace NYql::NTaskRunnerProxy::NPrivate
