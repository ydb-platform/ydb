#pragma once

#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/actors/core/log_iface.h>

namespace NKikimr::NConsole {

void SetGRpcLibraryFunction();
void EnableGRpcTracersEnable();
void SetGRpcLibraryLogVerbosity(NActors::NLog::EPriority prio);

} // namespace NKikimr::NGRpcService
