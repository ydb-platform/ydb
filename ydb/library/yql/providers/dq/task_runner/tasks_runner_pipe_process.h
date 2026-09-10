#pragma once

#include <util/datetime/base.h>

#include <util/generic/function.h>
#include <util/generic/maybe.h>

#include <util/system/shellcommand.h>

namespace NYql::NTaskRunnerProxy::NPrivate {

using TKillFunction = std::function<int(int, int)>;
using TWaitPidFunction = std::function<int(int, int*, int)>;

bool CleanupChildProcess(
    int* pid,
    TDuration timeout,
    const TKillFunction& killFunction,
    const TWaitPidFunction& waitPidFunction);

bool IsShellCommandSuccessful(
    TShellCommand::ECommandStatus status,
    const TMaybe<int>& exitCode);

} // namespace NYql::NTaskRunnerProxy::NPrivate
