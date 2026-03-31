
#include "storage_transport.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId THostConnection::GetServiceId() const
{
    return NKikimr::MakeBlobStorageDDiskId(
        DDiskId.NodeId,
        DDiskId.PDiskId,
        DDiskId.DDiskSlotId);
}

bool THostConnection::IsConnected() const
{
    return Credentials.DDiskInstanceGuid.has_value();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
