#include "service.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeStorageServiceId()
{
    return TActorId(0, "blk-service");
}

}   // namespace NYdb::NBS::NStorage
