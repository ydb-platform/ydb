#include "log_backend.h"
#include "json_envelope.h"
#include "log_backend_build.h"
#include <ydb/core/base/counters.h>

#include <util/system/mutex.h>

namespace NKikimr {

class TLogBackendWithJsonEnvelope : public TLogBackend {
public:
    TLogBackendWithJsonEnvelope(const TString& jsonEnvelope, THolder<TLogBackend> logBackend)
        : JsonEnvelope(jsonEnvelope)
        , LogBackend(std::move(logBackend))
    {}

    void WriteData(const TLogRecord& rec) override {
        try {
            TLogRecord record = rec;
            TString data = JsonEnvelope.ApplyJsonEnvelope(TStringBuf(record.Data, record.Len));
            record.Data = data.data();
            record.Len = data.size();
            LogBackend->WriteData(record);
        } catch (const std::exception& ex) {
            try {
                TStringBuilder error;
                error << "Exception on writing audit log line with json envelope: " << ex.what();
                TString data = JsonEnvelope.ApplyJsonEnvelope(error);
                TLogRecord record = rec;
                record.Data = data.data();
                record.Len = data.size();
                LogBackend->WriteData(record);
            } catch (...) {
            }
            throw;
        }
    }

    void ReopenLog() override {
        LogBackend->ReopenLog();
    }

    void ReopenLogNoFlush() override {
        LogBackend->ReopenLogNoFlush();
    }

    ELogPriority FiltrationLevel() const override {
        return LogBackend->FiltrationLevel();
    }

    size_t QueueSize() const override {
        return LogBackend->QueueSize();
    }

private:
    const TJsonEnvelope JsonEnvelope;
    const THolder<TLogBackend> LogBackend;
};

TAutoPtr<TLogBackend> CreateLogBackendWithUnifiedAgent(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters)
{
    if (runConfig.AppConfig.HasLogConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        const auto& dnConfig = runConfig.AppConfig.GetDynamicNameserviceConfig();
        TAutoPtr<TLogBackend> logBackend = TLogBackendBuildHelper::CreateLogBackendFromLogConfig(logConfig);
        if (logConfig.HasUAClientConfig()) {
            const auto& uaClientConfig = logConfig.GetUAClientConfig();
            auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
            auto logName = uaClientConfig.GetLogName();
            auto maxStaticNodeId = dnConfig.GetMaxStaticNodeId();
            TAutoPtr<TLogBackend> uaLogBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(
                uaClientConfig,
                uaCounters,
                logName,
                runConfig.NodeId <= maxStaticNodeId ? "static" : "slot",
                runConfig.TenantName,
                logConfig.HasClusterName() ? logConfig.GetClusterName() : ""
            );
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
        const auto& dnConfig = runConfig.AppConfig.GetDynamicNameserviceConfig();
        const auto& uaClientConfig = logConfig.GetUAClientConfig();
        auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
        auto logName = meteringConfig.HasLogName()
            ? meteringConfig.GetLogName()
            : uaClientConfig.GetLogName();
        auto maxStaticNodeId = dnConfig.GetMaxStaticNodeId();
        TAutoPtr<TLogBackend> uaLogBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(
            uaClientConfig,
            uaCounters,
            logName,
            runConfig.NodeId <= maxStaticNodeId ? "static" : "slot",
            runConfig.TenantName,
            logConfig.HasClusterName() ? logConfig.GetClusterName() : ""
        );
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
        const auto& dnConfig = runConfig.AppConfig.GetDynamicNameserviceConfig();
        const auto& uaClientConfig = logConfig.GetUAClientConfig();
        auto uaCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "ua_client");
        auto logName = runConfig.AppConfig.GetAuditConfig().GetUnifiedAgentBackend().HasLogName()
            ? runConfig.AppConfig.GetAuditConfig().GetUnifiedAgentBackend().GetLogName()
            : uaClientConfig.GetLogName();
        auto maxStaticNodeId = dnConfig.GetMaxStaticNodeId();
        logBackend = TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(
            uaClientConfig,
            uaCounters,
            logName,
            runConfig.NodeId <= maxStaticNodeId ? "static" : "slot",
            runConfig.TenantName,
            logConfig.HasClusterName() ? logConfig.GetClusterName() : ""
        );
    }

    return logBackend;
}

THolder<TLogBackend> MaybeWrapWithJsonEnvelope(THolder<TLogBackend> logBackend, const TString& jsonEnvelope) {
    Y_ASSERT(logBackend);
    if (jsonEnvelope.empty()) {
        return logBackend;
    }

    return MakeHolder<TLogBackendWithJsonEnvelope>(jsonEnvelope, std::move(logBackend));
}

TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> CreateAuditLogBackends(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr counters) {
    TMap<NKikimrConfig::TAuditConfig::EFormat, TVector<THolder<TLogBackend>>> logBackends;

    if (runConfig.AppConfig.HasAuditConfig()) {
        const auto& auditConfig = runConfig.AppConfig.GetAuditConfig();
        if (auditConfig.HasStderrBackend()) {
            auto logBackend = NActors::CreateStderrBackend();
            auto format = auditConfig.GetStderrBackend().GetFormat();
            logBackends[format].push_back(MaybeWrapWithJsonEnvelope(std::move(logBackend), auditConfig.GetStderrBackend().GetLogJsonEnvelope()));
        }

        if (auditConfig.HasFileBackend()) {
            auto logBackend = CreateAuditLogFileBackend(runConfig);
            if (logBackend) {
                auto format = auditConfig.GetFileBackend().GetFormat();
                logBackends[format].push_back(MaybeWrapWithJsonEnvelope(std::move(logBackend), auditConfig.GetFileBackend().GetLogJsonEnvelope()));
            }
        }

        if (auditConfig.HasUnifiedAgentBackend()) {
            auto logBackend = CreateAuditLogUnifiedAgentBackend(runConfig, counters);
            if (logBackend) {
                auto format = auditConfig.GetUnifiedAgentBackend().GetFormat();
                logBackends[format].push_back(MaybeWrapWithJsonEnvelope(std::move(logBackend), auditConfig.GetUnifiedAgentBackend().GetLogJsonEnvelope()));
            }
        }
    }

    return logBackends;
}


} // NKikimr
