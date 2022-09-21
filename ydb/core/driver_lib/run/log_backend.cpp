#include "log_backend.h"

namespace NKikimr {

TAutoPtr<TLogBackend> TLogBackendFactory::CreateLogBackendFromLogConfig(const TKikimrRunConfig& runConfig) {
    TAutoPtr<TLogBackend> logBackend;
    if (runConfig.AppConfig.HasLogConfig()) {
        const auto& logConfig = runConfig.AppConfig.GetLogConfig();
        if (logConfig.HasSysLog() && logConfig.GetSysLog()) {
            const TString& service = logConfig.GetSysLogService();
            logBackend = NActors::CreateSysLogBackend(service ? service : "KIKIMR", false, true);
        } else if (logConfig.HasBackendFileName()) {
            logBackend = NActors::CreateFileBackend(logConfig.GetBackendFileName());
        }
    }
    return logBackend;
}

TAutoPtr<TLogBackend> TLogBackendFactory::CreateLogBackend(
        const TKikimrRunConfig& runConfig,
        ::NMonitoring::TDynamicCounterPtr counters) {
    Y_UNUSED(counters);
    TAutoPtr<TLogBackend> logBackend = CreateLogBackendFromLogConfig(runConfig);
    if (logBackend) {
        return logBackend;
    } else {
        return NActors::CreateStderrBackend();
    }
}

} // NKikimr

