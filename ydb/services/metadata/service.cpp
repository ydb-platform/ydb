#include "service.h"

namespace NKikimr::NMetadataProvider {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

}
