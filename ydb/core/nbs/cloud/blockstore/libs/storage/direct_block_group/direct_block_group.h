#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/request.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NKikimr::NBsController;
using namespace NKikimr::NDDisk;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
{
private:
    struct TDDiskConnection
    {
        TDDiskId DDiskId;
        TQueryCredentials Credentials;

        TDDiskConnection(const TDDiskId& ddiskId,
                         const TQueryCredentials& credentials)
            : DDiskId(ddiskId)
            , Credentials(credentials)
        {}

        [[nodiscard]] TActorId GetServiceId() const
        {
            return MakeBlobStorageDDiskId(DDiskId.NodeId, DDiskId.PDiskId,
                                          DDiskId.DDiskSlotId);
        }
    };

    TVector<TDDiskConnection> DDiskConnections;
    TVector<TDDiskConnection> PersistentBufferConnections;

    ui64 RequestId = 0;
    std::list<std::shared_ptr<IRequest>> Requests;
    std::unordered_map<ui64, std::shared_ptr<IRequest>> RequestById;

public:
    TDirectBlockGroup(ui64 tabletId, ui32 generation,
                      const TVector<TDDiskId>& ddisksIds,
                      const TVector<TDDiskId>& persistentBufferDDiskIds);

    void EstablishConnections(const TActorContext& ctx);

    void HandleDDiskConnectResult(const NDDisk::TEvDDiskConnectResult::TPtr& ev,
                                  const TActorContext& ctx);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void SendWriteRequestsToDDisk(
        const TActorContext& ctx,
        const std::shared_ptr<TWriteRequest>& request);

    void HandleDDiskWriteResult(const NDDisk::TEvDDiskWriteResult::TPtr& ev,
                                const TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void SendReadRequestsToDDisk(const TActorContext& ctx,
                                 const std::shared_ptr<TReadRequest>& request);

    void HandleDDiskReadResult(const NDDisk::TEvDDiskReadResult::TPtr& ev,
                               const TActorContext& ctx);
};

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
