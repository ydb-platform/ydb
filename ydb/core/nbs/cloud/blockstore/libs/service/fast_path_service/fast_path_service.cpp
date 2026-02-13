#include "fast_path_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/direct_block_group_in_mem.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/base/counters.h>

namespace NYdb::NBS::NBlockStore {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

TFastPathService::TFastPathService(
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddiskIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount,
    ui32 storageMedia,
    const NYdb::NBS::NProto::TStorageConfig& storageConfig,
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters)
    : TraceSamplePeriod(
          TDuration::MilliSeconds(storageConfig.GetTraceSamplePeriod()))
    , CountersBase(
          counters ? GetServiceCounters(counters, "nbs_partitions") : nullptr)
{
    if (storageMedia == NYdb::NBS::NProto::EStorageMediaKind::STORAGE_MEDIA_MEMORY) {
        DirectBlockGroup = std::make_unique<NStorage::NPartitionDirect::TInMemoryDirectBlockGroup>(
          tabletId,
          generation,
          std::move(ddiskIds),
          std::move(persistentBufferDDiskIds),
          blockSize,
          blocksCount);
    } else {
        DirectBlockGroup = std::make_unique<NStorage::NPartitionDirect::TDirectBlockGroup>(
          tabletId,
          generation,
          std::move(ddiskIds),
          std::move(persistentBufferDDiskIds),
          blockSize,
          blocksCount);
    }

    // Build complete counter chain: ddiskPool -> tabletId -> subsystem:interface
    if (CountersBase) {
        CountersChain.emplace_back("ddiskPool", storageConfig.GetDDiskPoolName());
        CountersChain.emplace_back("tabletId", ToString(tabletId));

        auto finalCounters = CountersBase;
        for (const auto& [name, value] : CountersChain) {
            finalCounters = finalCounters->GetSubgroup(name, value);
        }

        auto cInterface = finalCounters->GetSubgroup("subsystem", "interface");
        auto cInterfaceWriteBlocks = cInterface->GetSubgroup("operation", "WriteBlocks");
        auto cInterfaceReadBlocks = cInterface->GetSubgroup("operation", "ReadBlocks");

        Counters.WriteBlocks.Requests = cInterfaceWriteBlocks->GetCounter("Requests", true);
        Counters.WriteBlocks.ReplyOk = cInterfaceWriteBlocks->GetCounter("ReplyOk", true);
        Counters.WriteBlocks.ReplyErr = cInterfaceWriteBlocks->GetCounter("ReplyErr", true);
        Counters.WriteBlocks.Bytes = cInterfaceWriteBlocks->GetCounter("Bytes", true);

        Counters.ReadBlocks.Requests = cInterfaceReadBlocks->GetCounter("Requests", true);
        Counters.ReadBlocks.ReplyOk = cInterfaceReadBlocks->GetCounter("ReplyOk", true);
        Counters.ReadBlocks.ReplyErr = cInterfaceReadBlocks->GetCounter("ReplyErr", true);
        Counters.ReadBlocks.Bytes = cInterfaceReadBlocks->GetCounter("Bytes", true);
    }

    // Set up counter callbacks
    DirectBlockGroup->SetWriteBlocksReplyCallback([this](bool ok) {
        Counters.WriteBlocks.Reply(ok);
    });

    DirectBlockGroup->SetReadBlocksReplyCallback([this](bool ok) {
        Counters.ReadBlocks.Reply(ok);
    });

    DirectBlockGroup->EstablishConnections();
}

NThreading::TFuture<TReadBlocksLocalResponse> TFastPathService::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    with_lock (Lock) {
        auto traceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                 // verbosity
            4095,               // timeToLive
            LastTraceTs,        // atomic counter for throttling
            NActors::TMonotonic::Now(),    // current monotonic time
            TraceSamplePeriod   // 100ms between samples
        );

        Counters.ReadBlocks.Request(request->Range.Size() * NStorage::NPartitionDirect::BlockSize);
        return DirectBlockGroup->ReadBlocksLocal(std::move(callContext), std::move(request), std::move(traceId));
    }
}

NThreading::TFuture<TWriteBlocksLocalResponse> TFastPathService::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    with_lock (Lock) {
        auto traceId = NWilson::TTraceId::NewTraceIdThrottled(
            15,                 // verbosity
            4095,               // timeToLive
            LastTraceTs,        // atomic counter for throttling
            NActors::TMonotonic::Now(),    // current monotonic time
            TraceSamplePeriod   // 100ms between samples
        );

        Counters.WriteBlocks.Request(request->Range.Size() * NStorage::NPartitionDirect::BlockSize);
        return DirectBlockGroup->WriteBlocksLocal(std::move(callContext), std::move(request), std::move(traceId));
    }
}

NThreading::TFuture<TZeroBlocksLocalResponse> TFastPathService::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    Y_UNUSED(callContext);
    Y_UNUSED(request);
    Y_ABORT_UNLESS(false, "ZeroBlocksLocal is not implemented");
    return NThreading::MakeFuture<TZeroBlocksLocalResponse>();
}

void TFastPathService::ReportIOError()
{
    // TODO: implement
}

}   // namespace NYdb::NBS::NBlockStore
