
#include "storage_transport.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId THostConnection::GetServiceId() const
{
    switch (ConnectionType) {
        case EConnectionType::PBuffer:
            return NKikimr::MakeBlobStoragePersistentBufferId(
                DDiskId.NodeId,
                DDiskId.PDiskId,
                DDiskId.DDiskSlotId);
        case EConnectionType::DDisk:
            return NKikimr::MakeBlobStorageDDiskId(
                DDiskId.NodeId,
                DDiskId.PDiskId,
                DDiskId.DDiskSlotId);
        default:
            Y_ABORT("Unknown connection type");
    }
}

bool THostConnection::IsConnected() const
{
    return Credentials.DDiskInstanceGuid.has_value();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
