#include "json_handlers.h"
#include "scheme_directory.h"

namespace NKikimr::NViewer {

void InitSchemeDirectoryHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/scheme/directory", new TJsonSchemeDirectoryHandler(), 2,
                        NActors::NAudit::EAuditableAction::MakeDirectory);
}

void InitSchemeJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitSchemeDirectoryHandler(jsonHandlers);
}

}
