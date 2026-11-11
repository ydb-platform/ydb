#include "service_id.h"

namespace NKikimr::NWorkloadManager {

NActors::TActorId MakeServiceId(ui32 nodeId) {
    const char name[12] = "kqp_workld";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

}   // namespace NKikimr::NWorkloadManager
