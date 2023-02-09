#include "log_backend.h"
#include "log_backend_build.h"
#include <ydb/core/base/counters.h>

namespace NKikimr {

TAutoPtr<TLogBackend> CreateLogBackendWithUnifiedAgent(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters)
{
    if (runConfig.AppConfig.HasLogConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        TAutoPtr<TLogBackend> logBackend = TLogBackendBuildHelper::CreateLogBackendFromLogConfig(logConfig);
        if (logConfig.HasUAClientConfig()) {
            const auto& uaClientConfig = logConfig.GetUAClientConfig();
            auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
            auto logName = uaClientConfig.GetLogName();
            TAutoPtr<TLogBackend> uaLogBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(uaClientConfig, uaCounters, logName);
            logBackend = logBackend ? NActors::CreateCompositeLogBackend({logBackend, uaLogBackend}) : uaLogBackend;
        }
        if (logBackend) {
            return logBackend;
        }
    }

    return NActors::CreateStderrBackend();
}

TAutoPtr<TLogBackend> CreateAuditLogBackendWithUnifiedAgent(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters)
{
    TAutoPtr<TLogBackend> logBackend;
    if (!runConfig.AppConfig.HasAuditConfig())
        return logBackend;

    const auto& auditConfig = runConfig.AppConfig.GetAuditConfig();
    if (auditConfig.HasAuditFilePath()) {
        const auto& filePath = auditConfig.GetAuditFilePath();
        try {
            logBackend = new TFileLogBackend(filePath);
        } catch (const TFileError& ex) {
            Cerr << "TAuditLogBackendFactoryWithUnifiedAgent: failed to open file '" << filePath << "': " << ex.what() << Endl;
            exit(1);
        }
    }

    if (auditConfig.GetUnifiedAgentEnable() && runConfig.AppConfig.HasLogConfig() && runConfig.AppConfig.GetLogConfig().HasUAClientConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        const auto& uaClientConfig = logConfig.GetUAClientConfig();
        auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
        auto logName = runConfig.AppConfig.GetAuditConfig().HasLogName()
            ? runConfig.AppConfig.GetAuditConfig().GetLogName()
            : uaClientConfig.GetLogName();
        TAutoPtr<TLogBackend> uaLogBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(uaClientConfig, uaCounters, logName);
        logBackend = logBackend ? NActors::CreateCompositeLogBackend({logBackend, uaLogBackend}) : uaLogBackend;
    }

    if (logBackend) {
        return logBackend;
    }
    return NActors::CreateStderrBackend();
}

} // NKikimr

