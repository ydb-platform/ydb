#include "service.h"

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NMetadata::NProvider {

NActors::TActorId MakeServiceId(const ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcMetaData");
}

} // namespace NKikimr::NMetadata::NProvider
