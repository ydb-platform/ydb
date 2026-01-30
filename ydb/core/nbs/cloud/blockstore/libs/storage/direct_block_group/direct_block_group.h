#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/request.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NKikimr::NBsController;

////////////////////////////////////////////////////////////////////////////////

// BlocksCount in one vChunk
constexpr size_t BlocksCount = 128 * 1024 * 1024 / 4096;

////////////////////////////////////////////////////////////////////////////////

class TDirectBlockGroup
{
private:
    struct TBlockMeta {
        TVector<ui64> LsnByPersistentBufferIndex;
        TVector<bool> IsFlushedToDDiskByPersistentBufferIndex;

        explicit TBlockMeta(size_t persistentBufferCount)
            : LsnByPersistentBufferIndex(persistentBufferCount, 0)
            , IsFlushedToDDiskByPersistentBufferIndex(persistentBufferCount, false)
        {}

        void OnWriteCompleted(const TWriteRequest::TPersistentBufferWriteMeta& writeMeta)
        {
            LsnByPersistentBufferIndex[writeMeta.Index] = writeMeta.Lsn;
            IsFlushedToDDiskByPersistentBufferIndex[writeMeta.Index] = false;
        }

        [[nodiscard]] bool WriteMetaIsOutdated(const TVector<TWriteRequest::TPersistentBufferWriteMeta>& writesMeta) const
        {
            for (const auto& meta : writesMeta) {
                if (LsnByPersistentBufferIndex[meta.Index] > meta.Lsn) {
                    return true;
                }
            }

            return false;
        }

        void OnFlushCompleted(bool isErase, size_t persistentBufferIndex, ui64 lsn)
        {
            if (!isErase && LsnByPersistentBufferIndex[persistentBufferIndex] == lsn) {
                LsnByPersistentBufferIndex[persistentBufferIndex] = 0;
                IsFlushedToDDiskByPersistentBufferIndex[persistentBufferIndex] = true;
            }
        }

        [[nodiscard]] bool IsWrited() const
        {
            return IsFlushedToDDisk() || LsnByPersistentBufferIndex[0] != 0;
        }

        [[nodiscard]] bool IsFlushedToDDisk() const
        {
            return IsFlushedToDDiskByPersistentBufferIndex[0];
        }
    };

    struct TDDiskConnection
    {
        TDDiskId DDiskId;
        NDDisk::TQueryCredentials Credentials;

        TDDiskConnection(const TDDiskId& ddiskId,
                         const NDDisk::TQueryCredentials& credentials)
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

    ui64 TabletId;
    ui32 Generation;
    ui64 RequestId = 0;
    std::unordered_map<ui64, std::shared_ptr<IRequest>> RequestById;
    TVector<TBlockMeta> BlocksMeta;

public:
    TDirectBlockGroup(ui64 tabletId, ui32 generation,
                      const TVector<TDDiskId>& ddisksIds,
                      const TVector<TDDiskId>& persistentBufferDDiskIds);

    void EstablishConnections(const TActorContext& ctx);

    void HandleDDiskConnectResult(
        const NDDisk::TEvConnectResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void SendWriteRequestsToDDisk(
        const TActorContext& ctx,
        const std::shared_ptr<TWriteRequest>& request);

    void HandlePersistentBufferWriteResult(
        const NDDisk::TEvWritePersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);

    void RequestBlockFlush(
        const TActorContext& ctx,
        const TWriteRequest& request,
        bool isErase,
        NWilson::TTraceId traceId);

    void HandlePersistentBufferFlushResult(
        const NDDisk::TEvFlushPersistentBufferResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void SendReadRequestsToPersistentBuffer(
        const TActorContext& ctx,
        const std::shared_ptr<TReadRequest>& request);

    template <typename TEvent>
    void HandleReadResult(
        const typename TEvent::TPtr& ev,
        const TActorContext& ctx);
};

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
