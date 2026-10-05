
#include "storage_transport.h"

#include "ic_direct_storage_transport.h"
#include "ic_storage_transport.h"
#include "ic_storage_transport_actor.h"

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
    }
}

bool THostConnection::IsConnected() const
{
    return Credentials.DDiskInstanceGuid.has_value();
}

TString THostConnection::DebugPrint() const
{
    TStringBuilder result;
    result << ToString(ConnectionType) << " ddisk:[" << DDiskId.ToString()
           << "] cred:[guid:" << Credentials.DDiskInstanceGuid.value_or(0)
           << " seqNo:" << Credentials.DDiskSessionSeqNo << "]";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TStorageTransportPtr CreateStorageTransport(
    NActors::TActorSystem* actorSystem,
    const TDiskDescription& diskDescription,
    ui32 dbgIndex,
    bool useDirectSessionTransport,
    bool enableChecksums)
{
    if (useDirectSessionTransport) {
        return CreateDirectStorageTransport(
            actorSystem,
            diskDescription,
            dbgIndex,
            enableChecksums);
    }

    return std::make_shared<TICStorageTransport>(
        actorSystem,
        CreateTransportActor(diskDescription, dbgIndex, enableChecksums));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
