#include "service.h"

namespace NKikimr::NMetadataProvider {

NActors::TActorId MakeServiceId(ui32 node) {
    return NActors::TActorId(node, "SrvcMetaData");
}

}
