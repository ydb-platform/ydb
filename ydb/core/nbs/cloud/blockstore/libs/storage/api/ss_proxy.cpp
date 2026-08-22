#include "ss_proxy.h"

namespace NYdb::NBS::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeSSProxyServiceId()
{
    return TActorId(0, "nbs-ssproxy");
}

}   // namespace NYdb::NBS::NStorage
