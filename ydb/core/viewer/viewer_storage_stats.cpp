#include "json_handlers.h"
#include "viewer_storage_stats.h"

namespace NKikimr::NViewer {

void InitViewerStorageStatsJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/viewer/storage_stats", new TJsonHandler<TJsonStorageStats>(TJsonStorageStats::GetSwagger()), 2);
}

} // namespace NKikimr::NViewer
