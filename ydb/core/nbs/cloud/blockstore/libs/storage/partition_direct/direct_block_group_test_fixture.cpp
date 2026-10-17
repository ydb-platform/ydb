#include "direct_block_group_test_fixture.h"

#include "partition_direct_service_mock.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TBlockedDetectedState TDBGFixture::GetBlockedDetected(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    THostIndex hostIndex,
    TDuration waitTimeout)
{
    return RunOnExecutor(
               executor,
               [dbg, hostIndex]
               {
                   return TBlockedDetectedState{
                       .DDiskSessionBroken =
                           dbg->Connections.GetDDisk(hostIndex).SessionState ==
                           EDDiskSessionState::Broken,
                       .BlockedGenerationDetected =
                           dbg->BlockedGenerationDetected};
               })
        .GetValue(waitTimeout);
}

std::array<size_t, MaxHostCount> TDBGFixture::CountDDisksByHostDebugOnly(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    EDDiskBalanceStrategy strategy,
    TDuration waitTimeout)
{
    return RunOnExecutor(
               executor,
               [dbg, strategy]
               {
                   const auto allowedForBalancing =
                       dbg->GetBalancingAllowedHosts();
                   return dbg->CountDDisksByHost(strategy, allowedForBalancing);
               })
        .GetValue(waitTimeout);
}

TVector<ui64> TDBGFixture::ReadAllDDiskSeqNos(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    TDuration waitTimeout)
{
    return RunOnExecutor(
               executor,
               [&]
               {
                   TVector<ui64> result;
                   for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
                       result.push_back(
                           dbg->Connections.GetDDisk(i).ConfirmedSessionSeqNo);
                   }
                   return result;
               })
        .GetValue(waitTimeout);
}

ui64 TDBGFixture::GetDDiskSessionSeqNo(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    size_t index,
    TDuration waitTimeout)
{
    return RunOnExecutor(
               executor,
               [&] {
                   return dbg->Connections.GetDDisk(index)
                       .ConfirmedSessionSeqNo;
               })
        .GetValue(waitTimeout);
}

std::optional<NKikimr::NDDisk::TConnectionToken>
TDBGFixture::GetConnectionToken(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    NTransport::THostConnection::EConnectionType connectionType,
    size_t index,
    TDuration waitTimeout)
{
    return RunOnExecutor(
               executor,
               [dbg, connectionType, index]
               {
                   return dbg->Connections.Get(connectionType, index)
                       .HostConnection.Credentials.ConnectionToken;
               })
        .GetValue(waitTimeout);
}

[[nodiscard]] std::shared_ptr<TDirectBlockGroup>
TDBGFixture::MakeDirectBlockGroup(
    const TExecutorPtr& executor,
    NStorage::NTransport::TStorageTransportPtr transport,
    const TVector<NKikimr::NBsController::TDDiskId>& ddisksIds,
    const TVector<NKikimr::NBsController::TDDiskId>& pbufferIds,
    size_t directBlockGroupIndex,
    ui32 dbgConnectionsConfigGeneration) const
{
    return std::make_shared<TDirectBlockGroup>(
        CreateArenaAllocator(),
        Runtime->GetActorSystem(0),
        std::make_shared<TStorageConfig>(StorageServiceConfig),
        executor,
        DiskDescription,
        DefaultBlockSize,
        directBlockGroupIndex,
        ddisksIds,
        pbufferIds,
        TVector(ddisksIds.size(), EHostHealth::Online),
        dbgConnectionsConfigGeneration,
        std::move(transport),
        nullptr);
}

NThreading::TFuture<void> TDBGFixture::RunAndGetInitialReady(
    const std::shared_ptr<TDirectBlockGroup>& dbg,
    bool dropScheduledCallbacks)
{
    auto service =
        std::make_shared<TPartitionDirectServiceMock>(dropScheduledCallbacks);
    if (Service) {
        OldServices.push_back(std::move(Service));
    }
    Service = service;

    return dbg->Run(TraceService.get(), service.get());
}

size_t TDBGFixture::ReplyUpdateRequests()
{
    auto requests = std::move(Service->UpdateConfigRequests);
    for (auto& r: requests) {
        r.Promise.SetValue(EPersistResult::Success);
    }
    return requests.size();
}

void TDBGFixture::WaitDirtyMapReady(
    const TExecutorPtr& executor,
    const std::shared_ptr<TVChunk>& vchunk)
{
    UNIT_ASSERT(DoExecutorAndRuntimeWorkWithPredicate(
        executor,
        [&] { return TBaseFixture::IsDirtyMapReady(*vchunk); },
        DefaultWaitFutureTimeout));
}

void TDBGFixture::WriteBlock(
    const TExecutorPtr& executor,
    const std::shared_ptr<TVChunk>& vchunk,
    ui64 blockIndex)
{
    TString buffer(DefaultBlockSize, 'w');
    auto request = std::make_shared<TWriteBlocksLocalRequest>(TRequestHeaders{
        .VolumeConfig = VolumeConfig,
        .RequestId = 1,
        .Range = TBlockRange64::WithLength(blockIndex, 1)});
    request->Sglist =
        TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});

    const auto response = WaitFuture(
        executor,
        vchunk->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request),
            NWilson::TTraceId::NewTraceId(
                NWilson::TTraceId::MAX_VERBOSITY,
                NWilson::TTraceId::MAX_TIME_TO_LIVE)),
        DefaultWaitFutureTimeout);
    UNIT_ASSERT_C(!HasError(response.Error), FormatError(response.Error));
}

void TDBGFixture::WaitBarrierErases(
    const TExecutorPtr& executor,
    const std::shared_ptr<NTransport::TStorageTransportMock>& transport,
    size_t count)
{
    UNIT_ASSERT_C(
        DoExecutorAndRuntimeWorkWithPredicate(
            executor,
            [&] { return transport->BarrierErases.size() >= count; },
            DefaultWaitFutureTimeout),
        "barrier erases sent: " << transport->BarrierErases.size());
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
