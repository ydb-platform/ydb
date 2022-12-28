#include "log_backend.h"
#include <ydb/core/base/counters.h>

namespace NKikimr {

TAutoPtr<TLogBackend> TAuditLogBackendFactory::CreateLogBackend(
        const TKikimrRunConfig& runConfig,
        NMonitoring::TDynamicCounterPtr)
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
            Cerr << "TAuditLogBackendFactory: failed to open file '" << filePath << "': " << ex.what() << Endl;
            exit(1);
        }
    }

    if (logBackend) {
        return logBackend;
    }
    return NActors::CreateStderrBackend();
}

} // NKikimr
