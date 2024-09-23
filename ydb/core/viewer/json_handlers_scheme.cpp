#include "json_handlers.h"

#include "scheme_directory.h"

namespace NKikimr::NViewer {

void InitSchemeJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/scheme/directory", new TJsonSchemeDirectoryHandler());
}

}
