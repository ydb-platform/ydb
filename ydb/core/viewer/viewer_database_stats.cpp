#include "json_handlers.h"
#include "viewer_database_stats.h"

namespace NKikimr::NViewer {

void InitViewerDatabaseStatsJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/database_stats", new THttpHandler<TJsonDatabaseStats>());
}

} // namespace NKikimr::NViewer
