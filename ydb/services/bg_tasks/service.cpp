#include "service.h"

namespace NKikimr::NBackgroundTasks {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcBgrdTask");
}

}
