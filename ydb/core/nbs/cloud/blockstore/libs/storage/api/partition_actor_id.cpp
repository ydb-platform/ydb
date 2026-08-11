#include "partition_actor_id.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TString SerializePartitionActorId(const NActors::TActorId& actorId)
{
    return TStringBuilder()
           << "[" << actorId.NodeId() << ":" << actorId.PoolID() << ":"
           << actorId.LocalId() << ":" << actorId.Hint() << "]";
}

bool TryDeserializePartitionActorId(TStringBuf str, NActors::TActorId& actorId)
{
    if (str.size() < 5 || str.front() != '[' || str.back() != ']') {
        return false;
    }

    const TStringBuf body = str.substr(1, str.size() - 2);
    TStringBuf parts[4];
    size_t count = 0;
    TStringBuf rest = body;
    while (count < 4) {
        if (!rest.NextTok(':', parts[count])) {
            break;
        }
        ++count;
    }
    // Reject leftover text and the legacy 3-field "[node:localId:hint]" form.
    if (count != 4 || !rest.empty()) {
        return false;
    }

    ui32 nodeId = 0;
    ui32 poolId = 0;
    ui64 localId = 0;
    ui32 hint = 0;
    if (!TryFromString(parts[0], nodeId) || !TryFromString(parts[1], poolId) ||
        !TryFromString(parts[2], localId) || !TryFromString(parts[3], hint))
    {
        return false;
    }

    if (nodeId > NActors::TActorId::MaxNodeId ||
        poolId > NActors::TActorId::MaxPoolID)
    {
        return false;
    }

    actorId = NActors::TActorId(nodeId, poolId, localId, hint);
    return true;
}

}   // namespace NYdb::NBS::NBlockStore
