#pragma once

#include "direct_block_group.h"

#include <ydb/library/pdisk_io/sector_map.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of DirectBlockGroup that doesn't use DDisk

class TInMemoryDirectBlockGroup: public IDirectBlockGroup
{
private:
    TIntrusivePtr<NKikimr::NPDisk::TSectorMap> SectorMap;
    ui32 BlockSize;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
    std::vector<std::pair<TString, TString>> CountersChain;

    struct
    {
        struct
        {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;

            void Request(ui32 bytes = 0)
            {
                if (Requests) {
                    ++*Requests;
                }
                if (bytes && Bytes) {
                    *Bytes += bytes;
                }
            }

            void Reply(bool ok)
            {
                if (ok && ReplyOk) {
                    ++*ReplyOk;
                } else if (!ok && ReplyErr) {
                    ++*ReplyErr;
                }
            }
        } WriteBlocks, ReadBlocks;
    } Counters;

    std::function<void(bool)> WriteBlocksReplyCallback;
    std::function<void(bool)> ReadBlocksReplyCallback;

public:
    TInMemoryDirectBlockGroup(
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddisksIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds,
        ui32 blockSize,
        ui64 blocksCount);

    ~TInMemoryDirectBlockGroup() override = default;

    void EstablishConnections() override;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId) override;

    void SetWriteBlocksReplyCallback(
        std::function<void(bool)> callback) override
    {
        WriteBlocksReplyCallback = std::move(callback);
    }

    void SetReadBlocksReplyCallback(std::function<void(bool)> callback) override
    {
        ReadBlocksReplyCallback = std::move(callback);
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
