#include "json_local_rpc_handlers.h"

namespace NKikimr::NViewer {

void InitOperationJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitOperationLocalRpcJsonHandlers(jsonHandlers);
}

} // namespace NKikimr::NViewer
