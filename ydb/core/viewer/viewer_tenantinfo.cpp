#include "json_handlers.h"
#include "viewer_tenantinfo.h"

namespace NKikimr::NViewer {

void InitViewerTenantInfoJsonHandler(TJsonHandlers &handlers) {
    handlers.AddHandler("/viewer/tenantinfo", new THttpHandler<TJsonTenantInfo>(TJsonTenantInfo::GetSwagger()), 5);
}

} // namespace NKikimr::NViewer
