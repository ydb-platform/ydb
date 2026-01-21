#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NYdb::NBS;
using namespace NKikimr::NBsController;
using namespace NKikimr::NDDisk;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
{
private:
    struct TDDiskConnection {
        TDDiskId DDiskId;
        TQueryCredentials Credentials;

        TDDiskConnection(TDDiskId ddiskId, TQueryCredentials credentials)
            : DDiskId(ddiskId)
            , Credentials(credentials)
        {}

        [[nodiscard]] TActorId GetServiceId() const {
            return MakeBlobStorageDDiskId(
                DDiskId.NodeId,
                DDiskId.PDiskId,
                DDiskId.DDiskSlotId);
        }
    };

    enum class ERequestType {
        Write,
        Read
    };
    

    struct TRequest {
        TActorId Sender;
        ui64 StartIndex;
        ERequestType Type;
        ui8 AckCount = 0;
        ui8 DDiskMask = 0;

        TRequest(
            TActorId sender,
            ui64 startIndex,
            ERequestType type)
            : Sender(sender)
            , StartIndex(startIndex)
            , Type(type)
        {}
    };

    struct TWriteRequest : TRequest {
        TString Data;

        TWriteRequest(
            TActorId sender,
            ui64 startIndex,
            ERequestType type,
            TString data)
            : TRequest(sender, startIndex, type)
            , Data(std::move(data))
        {}
    };

    struct TReadRequest : TRequest {
        ui64 BlocksCount;

        TReadRequest(
            TActorId sender,
            ui64 startIndex,
            ERequestType type,
            ui64 blocksCount)
            : TRequest(sender, startIndex, type)
            , BlocksCount(blocksCount)
        {}
    };

    std::list<std::shared_ptr<TRequest>> Requests;
    TVector<std::unique_ptr<TDDiskConnection>> DDiskConnections;
    TVector<std::unique_ptr<TDDiskConnection>> PersistentBufferConnections;

    ui64 StorageRequestId = 0;
    std::unordered_map<ui64, std::pair<std::shared_ptr<TRequest>, ui64>> RequestByStorageRequestId;

public:
    TDirectBlockGroup(
        ui64 tabletId,
        ui32 generation,
        const TVector<TDDiskId>& ddisksIds,
        const TVector<TDDiskId>& persistentBufferDDiskIds);

    void EstablishConnections(const TActorContext& ctx);
    void HandleDDiskConnectResult(const NDDisk::TEvDDiskConnectResult::TPtr& ev, const TActorContext& ctx);

    void ProcessNextRequest(const TActorContext& ctx);

    void HandleWriteBlocksRequest(const TEvService::TEvWriteBlocksRequest::TPtr& ev, const TActorContext& ctx);
    void HandleReadBlocksRequest(const TEvService::TEvReadBlocksRequest::TPtr& ev, const TActorContext& ctx);

    void HandleDDiskWriteResult(const NDDisk::TEvDDiskWriteResult::TPtr& ev, const TActorContext& ctx);
    void HandleDDiskReadResult(const NDDisk::TEvDDiskReadResult::TPtr& ev, const TActorContext& ctx);

    void SendWriteRequestsToDDisk(const TActorContext& ctx, const std::shared_ptr<TWriteRequest>& request);
    void SendReadRequestsToDDisk(const TActorContext& ctx, const std::shared_ptr<TReadRequest>& request);
};

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
