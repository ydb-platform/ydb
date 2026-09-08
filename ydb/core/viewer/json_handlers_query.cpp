#include "json_local_rpc_handlers.h"

namespace NKikimr::NViewer {

void InitQueryJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitQueryLocalRpcJsonHandlers(jsonHandlers);
}

} // namespace NKikimr::NViewer
