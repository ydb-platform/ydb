#include "json_local_rpc_handlers.h"

namespace NKikimr::NViewer {

void InitSchemeJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitSchemeLocalRpcJsonHandlers(jsonHandlers);
}

}
