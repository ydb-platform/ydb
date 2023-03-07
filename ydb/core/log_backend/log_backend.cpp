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

TAutoPtr<TLogBackend> CreateMeteringLogBackendWithUnifiedAgent(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters)
{
    TAutoPtr<TLogBackend> logBackend;
    if (!runConfig.AppConfig.HasMeteringConfig())
        return logBackend;

    const auto& meteringConfig = runConfig.AppConfig.GetMeteringConfig();
    if (meteringConfig.HasMeteringFilePath()) {
        const auto& filePath = meteringConfig.GetMeteringFilePath();
        try {
            logBackend = new TFileLogBackend(filePath);
        } catch (const TFileError& ex) {
            Cerr << "CreateMeteringLogBackendWithUnifiedAgent: failed to open file '" << filePath << "': " << ex.what() << Endl;
            exit(1);
        }
    }

    if (meteringConfig.GetUnifiedAgentEnable() && runConfig.AppConfig.HasLogConfig() && runConfig.AppConfig.GetLogConfig().HasUAClientConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        const auto& uaClientConfig = logConfig.GetUAClientConfig();
        auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
        auto logName = meteringConfig.HasLogName()
            ? meteringConfig.GetLogName()
            : uaClientConfig.GetLogName();
        TAutoPtr<TLogBackend> uaLogBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(uaClientConfig, uaCounters, logName);
        logBackend = logBackend ? NActors::CreateCompositeLogBackend({logBackend, uaLogBackend}) : uaLogBackend;
    }

    if (logBackend) {
        return logBackend;
    }
    return NActors::CreateStderrBackend();
}

TAutoPtr<TLogBackend> CreateAuditLogFileBackend(
        const TKikimrRunConfig& runConfig)
{
    TAutoPtr<TLogBackend> logBackend;
    if (!runConfig.AppConfig.HasAuditConfig())
        return logBackend;

    const auto& auditConfig = runConfig.AppConfig.GetAuditConfig();
    if (auditConfig.HasFileBackend() && auditConfig.GetFileBackend().HasFilePath()) {
        const auto& filePath = auditConfig.GetFileBackend().GetFilePath();
        try {
            logBackend = new TFileLogBackend(filePath);
        } catch (const TFileError& ex) {
            Cerr << "CreateAuditLogFileBackend: failed to open file '" << filePath << "': " << ex.what() << Endl;
            exit(1);
        }
    }

    return logBackend;
}


TAutoPtr<TLogBackend> CreateAuditLogUnifiedAgentBackend(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters)
{
    TAutoPtr<TLogBackend> logBackend;
    if (!runConfig.AppConfig.HasAuditConfig())
        return logBackend;

    const auto& auditConfig = runConfig.AppConfig.GetAuditConfig();
    if (auditConfig.HasUnifiedAgentBackend() && runConfig.AppConfig.HasLogConfig() && runConfig.AppConfig.GetLogConfig().HasUAClientConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        const auto& uaClientConfig = logConfig.GetUAClientConfig();
        auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
        auto logName = runConfig.AppConfig.GetAuditConfig().GetUnifiedAgentBackend().HasLogName()
            ? runConfig.AppConfig.GetAuditConfig().GetUnifiedAgentBackend().GetLogName()
            : uaClientConfig.GetLogName();
        logBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(uaClientConfig, uaCounters, logName);
    }

    return logBackend;
}

TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> CreateAuditLogBackends(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters) {
    TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> logBackends;
    if (runConfig.AppConfig.HasAuditConfig() && runConfig.AppConfig.GetAuditConfig().HasStderrBackend()) {
        auto logBackend = NActors::CreateStderrBackend();
        auto format = runConfig.AppConfig.GetAuditConfig().GetStderrBackend().GetFormat();
        logBackends[format].push_back(std::move(logBackend));
    }

    if (runConfig.AppConfig.HasAuditConfig() && runConfig.AppConfig.GetAuditConfig().HasFileBackend()) {
        auto logBackend = CreateAuditLogFileBackend(runConfig);
        if (logBackend) {
            auto format = runConfig.AppConfig.GetAuditConfig().GetFileBackend().GetFormat();
            logBackends[format].push_back(std::move(logBackend));
        }
    }

    if (runConfig.AppConfig.HasAuditConfig() && runConfig.AppConfig.GetAuditConfig().HasUnifiedAgentBackend()) {
        auto logBackend = CreateAuditLogUnifiedAgentBackend(runConfig, counters);
        if (logBackend) {
            auto format = runConfig.AppConfig.GetAuditConfig().GetUnifiedAgentBackend().GetFormat();
            logBackends[format].push_back(std::move(logBackend));
        }
    }


    return logBackends;
}


} // NKikimr

