#pragma once

#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/direct_block_group.h>

#include <ydb/core/mind/bscontroller/types.h>
#include <ydb/core/mon/mon.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TFastPathService
    : public IStorage
    , public std::enable_shared_from_this<TFastPathService>
{
private:
    TMutex Lock;
    std::unique_ptr<NStorage::NPartitionDirect::IDirectBlockGroup> DirectBlockGroup;

    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    // Throttle trace ID creation to avoid overwhelming the tracing system
    TDuration TraceSamplePeriod;

    TIntrusivePtr<NMonitoring::TDynamicCounters> CountersBase;
    std::vector<std::pair<TString, TString>> CountersChain;

    struct {
        struct {
            NMonitoring::TDynamicCounters::TCounterPtr Requests;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
            NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
            NMonitoring::TDynamicCounters::TCounterPtr Bytes;

            void Request(ui32 bytes = 0) {
                if (Requests) {
                    ++*Requests;
                }
                if (bytes && Bytes) {
                    *Bytes += bytes;
                }
            }

            void Reply(bool ok) {
                if (ok && ReplyOk) {
                    ++*ReplyOk;
                } else if (!ok && ReplyErr) {
                    ++*ReplyErr;
                }
            }
        } WriteBlocks, ReadBlocks;
    } Counters;

public:
    TFastPathService(
        ui64 tabletId,
        ui32 generation,
        TVector<NKikimr::NBsController::TDDiskId> ddiskIds,
        TVector<NKikimr::NBsController::TDDiskId> persistentBufferDDiskIds,
        ui32 blockSize,
        ui64 blocksCount,
        ui32 storageMedia,
        const NYdb::NBS::NProto::TStorageConfig& storageConfig,
        const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters = nullptr);

    ~TFastPathService() override = default;

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;
};

}   // namespace NYdb::NBS::NBlockStore
