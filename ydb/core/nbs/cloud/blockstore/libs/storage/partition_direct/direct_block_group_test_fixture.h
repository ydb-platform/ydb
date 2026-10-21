#pragma once

#include "base_test_fixture.h"
#include "direct_block_group_impl.h"
#include "partition_direct_service_mock.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/trace_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/volume_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/ic_storage_transport_test_adapter.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/storage_transport_test_fixture.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

struct TBlockedDetectedState
{
    bool DDiskSessionBroken = false;
    bool BlockedGenerationDetected = false;
};

struct TDBGFixture: public NTransport::NTestLib::TStorageTransportTestFixture
{
    NProto::TStorageServiceConfig StorageServiceConfig;
    TDiskDescription DiskDescription{
        .DiskId = "disk-id",
        .TabletId = 100,
        .Generation = 1};
    TVolumeConfigPtr VolumeConfig = std::make_shared<TVolumeConfig>(
        "disk-id",
        DefaultBlockSize,
        65536,   // blockCount
        1024,    // blocksPerStripe
        DefaultVChunkSize);
    std::shared_ptr<TTraceServiceMock> TraceService =
        std::make_shared<TTraceServiceMock>();
    std::shared_ptr<TPartitionDirectServiceMock> Service;
    // Mock services created by RunAndGetInitialReady(). Kept alive for the
    // whole test because TDirectBlockGroup::Run() stores a raw pointer to the
    // service.
    TVector<IPartitionDirectServicePtr> OldServices;

    // Creates a mock service and starts the dbg.
    NThreading::TFuture<void> RunAndGetInitialReady(
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        bool dropScheduledCallbacks = true);

    [[nodiscard]] static TBlockedDetectedState GetBlockedDetected(
        const TExecutorPtr& executor,
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        THostIndex hostIndex,
        TDuration waitTimeout);
    [[nodiscard]] static std::array<size_t, MaxHostCount>
    CountDDisksByHostDebugOnly(
        const TExecutorPtr& executor,
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        EDDiskBalanceStrategy strategy,
        TDuration waitTimeout);
    [[nodiscard]] static TVector<ui64> ReadAllDDiskSeqNos(
        const TExecutorPtr& executor,
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        TDuration waitTimeout);
    [[nodiscard]] ui64 GetDDiskSessionSeqNo(
        const TExecutorPtr& executor,
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        size_t index,
        TDuration waitTimeout);
    [[nodiscard]] static std::optional<NKikimr::NDDisk::TConnectionToken>
    GetConnectionToken(
        const TExecutorPtr& executor,
        const std::shared_ptr<TDirectBlockGroup>& dbg,
        NTransport::THostConnection::EConnectionType connectionType,
        size_t index,
        TDuration waitTimeout);

    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        NTransport::TStorageTransportPtr transport,
        const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
        const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds,
        size_t directBlockGroupIndex = 0,
        ui32 dbgConnectionsConfigGeneration = 0) const;

    template <typename TTransport>
        requires std::derived_from<TTransport, NTransport::IStorageTransport>
    [[nodiscard]] std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::shared_ptr<TTransport> transport,
        size_t directBlockGroupIndex = 0,
        ui32 dbgConnectionsConfigGeneration = 0) const
    {
        auto ddisks = transport->GetDDiskIds();
        auto pbuffers = transport->GetPBufferIds();

        return MakeDirectBlockGroup(
            executor,
            std::move(transport),
            ddisks,
            pbuffers,
            directBlockGroupIndex,
            dbgConnectionsConfigGeneration);
    }

    // Sets all response Promises for update configs requests. Returns executed
    // requests count.
    size_t ReplyUpdateRequests();

    // Waits until the vchunk has restored its dirty map.
    void WaitDirtyMapReady(
        const TExecutorPtr& executor,
        const std::shared_ptr<TVChunk>& vchunk);

    // Writes one block through the vchunk and waits for the reply.
    void WriteBlock(
        const TExecutorPtr& executor,
        const std::shared_ptr<TVChunk>& vchunk,
        ui64 blockIndex);

    // Waits until the transport has sent `count` barrier erases.
    void WaitBarrierErases(
        const TExecutorPtr& executor,
        const std::shared_ptr<NTransport::TStorageTransportMock>& transport,
        size_t count);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
