#include "subsystem.h"

#include <ydb/core/protos/config.pb.h>
#include <util/string/cast.h>

namespace NKikimr::NActorSystemMonitoring {

TConfig MakeConfig(const NKikimrConfig::TActorSystemConfig& systemConfig,
        ui32 executorPool, bool autoConfigured) {
    TConfig monitoring;
    monitoring.ExecutorPool = executorPool;
    monitoring.AutoConfigured = autoConfigured;
    auto systemParameters = systemConfig;
    systemParameters.ClearExecutor();
    monitoring.SystemParameters = systemParameters.DebugString();
    for (const auto& executor : systemConfig.GetExecutor()) {
        auto& pool = monitoring.Pools.emplace_back();
        pool.Name = executor.GetName();
        pool.IsIo = executor.GetType() == NKikimrConfig::TActorSystemConfig::TExecutor::IO;
        if (executor.HasThreads()) {
            pool.Threads = ToString(executor.GetThreads());
        }
        if (executor.HasMinThreads()) {
            pool.MinThreads = ToString(executor.GetMinThreads());
        }
        if (executor.HasMaxThreads()) {
            pool.MaxThreads = ToString(executor.GetMaxThreads());
        }
        if (executor.HasPriority()) {
            pool.Priority = ToString(executor.GetPriority());
        }
        if (executor.GetAllThreadsAreShared()) {
            pool.SharedThreads = "All";
        } else if (executor.HasHasSharedThread()) {
            pool.SharedThreads = executor.GetHasSharedThread() ? "One" : "None";
        }
        pool.Parameters = executor.DebugString();
    }
    return monitoring;
}

} // namespace NKikimr::NActorSystemMonitoring
