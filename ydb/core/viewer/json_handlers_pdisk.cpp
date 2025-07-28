#include "json_handlers.h"
#include "pdisk_info.h"
#include "pdisk_restart.h"
#include "pdisk_status.h"

namespace NKikimr::NViewer {

void InitPDiskInfoJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/pdisk/info", new TJsonHandler<TPDiskInfo>(TPDiskInfo::GetSwagger()));
}

void InitPDiskRestartJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/pdisk/restart", new TJsonHandler<TJsonPDiskRestart>(TJsonPDiskRestart::GetSwagger()), 1,
                        NActors::NAudit::EAuditableAction::RestartPDisk);
}

void InitPDiskStatusJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/pdisk/status", new TJsonHandler<TPDiskStatus>(TPDiskStatus::GetSwagger()), 1,
                        NActors::NAudit::EAuditableAction::ChangePDiskStatus);
}

void InitPDiskJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitPDiskInfoJsonHandler(jsonHandlers);
    InitPDiskRestartJsonHandler(jsonHandlers);
    InitPDiskStatusJsonHandler(jsonHandlers);
}

}
