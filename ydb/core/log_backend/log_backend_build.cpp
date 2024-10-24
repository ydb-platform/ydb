#include "log_backend_build.h"
#include <util/system/getpid.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {

TAutoPtr<TLogBackend> TLogBackendBuildHelper::CreateLogBackendFromLogConfig(const NKikimrConfig::TLogConfig& logConfig, const TString& defaultIdent) {
    TAutoPtr<TLogBackend> logBackend;
    if (logConfig.HasSysLog() && logConfig.GetSysLog()) {
        const TString& service = logConfig.GetSysLogService();
        logBackend = NActors::CreateSysLogBackend(service ? service : defaultIdent, false, true);
    } else if (logConfig.HasBackendFileName()) {
        logBackend = NActors::CreateFileBackend(logConfig.GetBackendFileName());
    }
    return logBackend;
}

TAutoPtr<TLogBackend> TLogBackendBuildHelper::CreateLogBackendFromUAClientConfig(const NKikimrConfig::TUAClientConfig& uaClientConfig,
                                                                                 NMonitoring::TDynamicCounterPtr uaCounters,
                                                                                 const TString& logName, const TString& nodeType = "static",
                                                                                 const TString& tenant = "",
                                                                                 const TString& clusterName = "") {
    auto parameters = NUnifiedAgent::TClientParameters(uaClientConfig.GetUri())
        .SetCounters(uaCounters)
        .SetMaxInflightBytes(uaClientConfig.GetMaxInflightBytes());
    if (uaClientConfig.HasSharedSecretKey()) {
        parameters.SetSharedSecretKey(uaClientConfig.GetSharedSecretKey());
    }
    if (uaClientConfig.HasGrpcReconnectDelayMs()) {
        parameters.SetGrpcReconnectDelay(TDuration::MilliSeconds(uaClientConfig.GetGrpcReconnectDelayMs()));
    }
    if (uaClientConfig.HasGrpcSendDelayMs()) {
        parameters.SetGrpcSendDelay(TDuration::MilliSeconds(uaClientConfig.GetGrpcSendDelayMs()));
    }
    if (uaClientConfig.HasGrpcMaxMessageSize()) {
        parameters.SetGrpcMaxMessageSize(uaClientConfig.GetGrpcMaxMessageSize());
    }
    if (uaClientConfig.HasClientLogFile()) {
        TLog log(uaClientConfig.GetClientLogFile(),
                static_cast<ELogPriority>(uaClientConfig.GetClientLogPriority()));
        parameters.SetLog(log);
    }

    auto sessionParameters = NUnifiedAgent::TSessionParameters();
    sessionParameters.Meta.ConstructInPlace();
    (*sessionParameters.Meta)["_pid"] = ToString(GetPID());
    if (logName) {
        (*sessionParameters.Meta)["_log_name"] = logName;
    }

    if (nodeType) {
        (*sessionParameters.Meta)["node_type"] = nodeType;
    }

    if (tenant) {
        (*sessionParameters.Meta)["database"] = tenant;
    }

    if (clusterName) {
        (*sessionParameters.Meta)["cluster"] = clusterName;
    }

    TAutoPtr<TLogBackend> uaLogBackend = MakeLogBackend(parameters, sessionParameters).Release();
    return uaLogBackend;
}

} // NKikimr
