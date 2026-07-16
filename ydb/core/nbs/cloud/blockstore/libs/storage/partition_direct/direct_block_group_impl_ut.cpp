#include "direct_block_group_test_fixture.h"
#include "vchunk.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/ic_storage_transport_test_adapter.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitTimeout = TDuration::Seconds(10);

using EConnectionType = NTransport::THostConnection::EConnectionType;
using TStorageTransportMock = NTransport::TStorageTransportMock;
using TICStorageTransportTestAdapter =
    NTransport::NTestLib::TICStorageTransportTestAdapter;
using TDDiskId = NBsController::TDDiskId;

////////////////////////////////////////////////////////////////////////////////

TVector<TDDiskId> MakeDDiskIds(ui32 baseNodeId, ui32 count)
{
    TVector<TDDiskId> ids;
    ids.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        ids.emplace_back(baseNodeId + i, 1, i);
    }
    return ids;
}

TGuardedSgList MakeSgList(TString& buffer)
{
    return TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});
}

// Unwraps a future-of-future for single-thread rests on TransportMock.
template <typename T>
T GetResponse(const TFuture<TFuture<T>>& outer, TDuration timeout = WaitTimeout)
{
    return outer.GetValue(timeout).GetValue(timeout);
}

// Resolves pending connect promises in the half-open range [from, to)
void ResolveConnects(
    TVector<TStorageTransportMock::TConnectPromise>& promises,
    size_t from,
    size_t to)
{
    for (size_t i = from; i < to; ++i) {
        promises[i].SetValue(TStorageTransportMock::MakeConnectResult());
    }
}

NWilson::TTraceId CreateTraceId()
{
    return NWilson::TTraceId::NewTraceId(
        NWilson::TTraceId::MAX_VERBOSITY,
        NWilson::TTraceId::MAX_TIME_TO_LIVE);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectBlockGroupTest)
{
    // The initial-ready signal fires exactly once, only after the locked
    // quorum (3 of 5 DDisk sessions and PBuffers) is reached.
    Y_UNIT_TEST_F(ShouldSignalInitialReadyOnceLockedQuorumReached, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        const auto& ddisks = transport->GetDDiskIds();

        // All DDisk connects are deferred -> the sessions stay NotLocked until
        // the test resolves them.
        TVector<TStorageTransportMock::TConnectPromise> connectDDiskPromises;
        for (const auto& ddiskId: ddisks) {
            connectDDiskPromises.push_back(
                transport->SetPendingConnect(EConnectionType::DDisk, ddiskId));
        }

        const auto& pbuffers = transport->GetPBufferIds();
        TVector<TStorageTransportMock::TConnectPromise> connectPBufferPromises;
        for (size_t i = 2; i < pbuffers.size(); ++i) {
            connectPBufferPromises.push_back(transport->SetPendingConnect(
                EConnectionType::PBuffer,
                pbuffers[i]));
        }

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        auto initialReady = RunAndGetInitialReady(dbg);

        // Establish-connections has run, but no DDisk session is locked yet.
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // Resolve 3 ddisk sessions: still don't have pbuffer's quorum.
        ResolveConnects(connectDDiskPromises, 0, 3);
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // The third PBuffer connected
        ResolveConnects(connectPBufferPromises, 0, 1);
        DrainExecutor(executor);
        UNIT_ASSERT(initialReady.HasValue());
    }

    Y_UNIT_TEST_F(ShouldReadWriteOnQuorum, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();

        const auto& ddisks = transport->GetDDiskIds();
        // Hosts 0..2 connect immediately (default) and form the quorum; hosts
        // 3 and 4 stay pending.
        auto pendingHost3 =
            transport->SetPendingConnect(EConnectionType::DDisk, ddisks[3]);
        transport->SetPendingConnect(EConnectionType::DDisk, ddisks[4]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        // 3 immediate sessions -> quorum reached.
        WaitReady(initialReady);
        const auto range = TBlockRange64::WithLength(0, 1);

        // Read from a locked host completes right away.
        {
            TString readyBuffer(DefaultBlockSize, 'r');
            auto readyRead = RunOnExecutor(
                executor,
                [&]
                {
                    return dbg->ReadBlocksFromDDisk(
                        0,
                        0,
                        range,
                        MakeSgList(readyBuffer),
                        CreateTraceId());
                });
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                GetResponse(readyRead).Error.GetCode());
        }

        // Write to a locked host completes right away.
        {
            TString readyWriteBuffer(DefaultBlockSize, 'w');
            auto readyWrite = RunOnExecutor(
                executor,
                [&]
                {
                    return dbg->WriteBlocksToDDisk(
                        0,
                        1,
                        range,
                        MakeSgList(readyWriteBuffer),
                        CreateTraceId());
                });
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                GetResponse(readyWrite).Error.GetCode());
        }
    }

    // With a 3-of-5 quorum, reads/writes to a locked host pass through,
    // while a request to a still-connecting host is suspended until its session
    // is established.
    Y_UNIT_TEST_F(ShouldBlockDDiskIoUntilSessionEstablished, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();

        const auto& ddisks = transport->GetDDiskIds();
        // Hosts 0..2 connect immediately (default) and form the quorum; hosts
        // 3 and 4 stay pending.
        auto pendingHost3 =
            transport->SetPendingConnect(EConnectionType::DDisk, ddisks[3]);
        transport->SetPendingConnect(EConnectionType::DDisk, ddisks[4]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        // Three immediate sessions -> quorum reached.
        WaitReady(initialReady);
        const auto range = TBlockRange64::WithLength(0, 1);

        // Read from the still-connecting host 3 suspends inside the method, so
        // the outer future (carrying the returned future) is not resolved.
        TString pendingBuffer(DefaultBlockSize, 'p');
        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    3,
                    range,
                    MakeSgList(pendingBuffer),
                    CreateTraceId());
            });
        auto inFlightRead = WaitFuture(executor, pendingRead, WaitTimeout);
        DrainExecutor(executor);
        UNIT_ASSERT(!inFlightRead.HasValue());

        // Establishing the session unblocks the read.
        pendingHost3.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);
        auto response = WaitFuture(executor, inFlightRead, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response.Error.GetCode());
    }

    // The tablet-wide "all DBGs ready" gate (WaitAll over per-DBG
    // initial-ready futures) stays unresolved while any single DBG is below its
    // quorum, and resolves once every DBG reaches it.
    Y_UNIT_TEST_F(ShouldWaitAllDBGsInitiallyReady, TDBGFixture)
    {
        // DBG A: every DDisk session connects immediately -> ready after Run.
        auto executorA = MakeExecutor();
        auto transportA = std::make_unique<TStorageTransportMock>();
        ui32 baseNodeId = transportA->GetDDiskIds()[0].NodeId;
        auto dbgA = MakeDirectBlockGroup(executorA, std::move(transportA));

        // DBG B: every DDisk session is deferred -> not ready yet.
        auto executorB = MakeExecutor();
        auto transportB =
            std::make_unique<TStorageTransportMock>(baseNodeId + 100);

        const auto& ddisksB = transportB->GetDDiskIds();
        TVector<TStorageTransportMock::TConnectPromise> connectPromisesB;
        for (const auto& ddiskId: ddisksB) {
            connectPromisesB.push_back(
                transportB->SetPendingConnect(EConnectionType::DDisk, ddiskId));
        }
        auto dbgB = MakeDirectBlockGroup(executorB, std::move(transportB));

        // Mirror fast_path_service.cpp: WaitAll over per-DBG initial-ready
        // futures returned by Run().
        auto initialReadyA = RunAndGetInitialReady(dbgA);
        auto initialReadyB = RunAndGetInitialReady(dbgB);

        TVector<TFuture<void>> initialReadyFutures{
            initialReadyA,
            initialReadyB};
        auto allReady = NThreading::WaitAll(initialReadyFutures);

        // DBG A is ready; DBG B is not -> the aggregate gate is not resolved.
        WaitReady(initialReadyA);
        DrainExecutor(executorB);
        UNIT_ASSERT(!allReady.HasValue());

        // Bring DBG B to its quorum (3 of 5).
        ResolveConnects(connectPromisesB, 0, 2);
        DrainExecutor(executorB);
        UNIT_ASSERT(!allReady.HasValue());

        ResolveConnects(connectPromisesB, 2, 3);

        // Now every DBG is ready -> the aggregate gate resolves.
        WaitReady(allReady);
    }

    Y_UNIT_TEST_F(OracleShouldIgnoreCancelledError, TDBGFixture)
    {
        const auto ddiskHost = THostIndex(3);

        auto executor = MakeExecutor();
        auto dbg = MakeDirectBlockGroup(
            executor,
            std::make_unique<TStorageTransportMock>());

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        TString pendingBuffer(DefaultBlockSize, 'p');
        TGuardedSgList guardedSglist = MakeSgList(pendingBuffer);
        guardedSglist.Close();

        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,           // VChunkIndex
                    ddiskHost,   // host
                    TBlockRange64::WithLength(0, 3),
                    guardedSglist,
                    CreateTraceId());
            });
        auto readyReadResponse =
            pendingRead.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(
            E_CANCELLED,
            readyReadResponse.Error.GetCode());

        const auto& hostStat = dbg->GetOracle()->GetHostStatistics(ddiskHost);
        const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            hostStat.InflightCount(EOperation::ReadFromDDisk));
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
        UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveSuccessCount);
    }

    Y_UNIT_TEST_F(
        OracleShouldUpdateErrorCountersForAllPBuffersHosts,
        TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->WriteToManyPBufferStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        TString pendingBuffer(DefaultBlockSize, 'p');
        TGuardedSgList guardedSglist = MakeSgList(pendingBuffer);
        THostMask hosts;
        hosts.Set(1);
        hosts.Set(2);
        hosts.Set(3);

        auto pendingIndirectWrite = RunOnExecutor(
            executor,
            [&]
            {
                NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse>
                    promise = NThreading::NewPromise<
                        TDBGWriteBlocksToManyPBuffersResponse>();
                auto future = promise.GetFuture();
                TDirectBlockGroup::TWriteBlocksToManyPBuffersCallback cb =
                    [promise = std::move(promise)]   //
                    (TDBGWriteBlocksToManyPBuffersResponse r) mutable
                {
                    promise.SetValue(std::move(r));
                };

                dbg->WriteBlocksToManyPBuffers(
                    0,   // VChunkIndex
                    2,   // Coordinator
                    hosts,
                    100,   // lsn
                    TBlockRange64::WithLength(0, 3),
                    TDuration::Seconds(1),
                    guardedSglist,
                    CreateTraceId(),
                    cb);
                return future;
            });
        auto writeResponse =
            pendingIndirectWrite.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(3, writeResponse.Responses.size());
        for (const auto& response: writeResponse.Responses) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FAIL,
                response.Error.GetCode(),
                FormatError(response.Error));
        }

        auto checkHostStat = [&](ui32 hostIndex)
        {
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(hostIndex);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::WriteToPBuffer));
            UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveSuccessCount);
        };
        checkHostStat(1);
        checkHostStat(2);
        checkHostStat(3);
    }

    Y_UNIT_TEST_F(
        ShouldUpdatePBufferHostStatForCrossNodeFlushWhenError,
        TDBGFixture)
    {
        const auto pbufferHost = THostIndex(1);
        const auto ddiskHost = THostIndex(2);

        auto transport = std::make_unique<TStorageTransportMock>();
        transport->SyncWithPBufferStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;

        auto executor = MakeExecutor();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        auto pendingFlush = RunOnExecutor(
            executor,
            [&]
            {
                TVector<TPBufferSegment> segments;
                segments.push_back(
                    TPBufferSegment(100, TBlockRange64::WithLength(0, 3)));

                return dbg->SyncWithPBuffer(
                    10,   // VChunkIndex
                    pbufferHost,
                    ddiskHost,
                    segments,
                    CreateTraceId());
            });
        auto flushResponse =
            pendingFlush.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, flushResponse.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FAIL,
            flushResponse.Errors[0].GetCode(),
            FormatError(flushResponse.Errors[0]));

        {   // Should not count an error for the pbuffer host
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(pbufferHost);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::FlushCrossNode));
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveSuccessCount);
        }
        {   // Should count an error for the ddisk host
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(ddiskHost);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::FlushCrossNode));
            UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveSuccessCount);
        }
    }

    Y_UNIT_TEST_F(
        ShouldUpdateBothHostsStatForCrossNodeFlushWhenSuccess,
        TDBGFixture)
    {
        const auto pbufferHost = THostIndex(1);
        const auto ddiskHost = THostIndex(2);

        auto executor = MakeExecutor();
        auto dbg = MakeDirectBlockGroup(
            executor,
            std::make_unique<TStorageTransportMock>());

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        auto pendingFlush = RunOnExecutor(
            executor,
            [&]
            {
                TVector<TPBufferSegment> segments;
                segments.push_back(
                    TPBufferSegment(100, TBlockRange64::WithLength(0, 3)));

                return dbg->SyncWithPBuffer(
                    10,   // VChunkIndex
                    pbufferHost,
                    ddiskHost,
                    segments,
                    CreateTraceId());
            });
        auto flushResponse =
            pendingFlush.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, flushResponse.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            flushResponse.Errors[0].GetCode(),
            FormatError(flushResponse.Errors[0]));

        {   // Should count a success for the pbuffer host
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(pbufferHost);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::FlushCrossNode));
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
            UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveSuccessCount);
        }
        {   // Should count a success for the ddisk host
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(ddiskHost);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::FlushCrossNode));
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
            UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveSuccessCount);
        }
    }

    Y_UNIT_TEST_F(
        ShouldSendCoordinatorDDiskFirstInWriteToManyPBuffers,
        TDBGFixture)
    {
        const auto coordinatorHost = THostIndex(3);

        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        TString pendingBuffer(DefaultBlockSize, 'p');
        TGuardedSgList guardedSglist = MakeSgList(pendingBuffer);
        THostMask hosts;
        hosts.Set(1);
        hosts.Set(coordinatorHost);
        hosts.Set(4);

        auto pendingWrite = RunOnExecutor(
            executor,
            [&]
            {
                NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse>
                    promise = NThreading::NewPromise<
                        TDBGWriteBlocksToManyPBuffersResponse>();
                auto future = promise.GetFuture();
                TDirectBlockGroup::TWriteBlocksToManyPBuffersCallback cb =
                    [promise = std::move(promise)]   //
                    (TDBGWriteBlocksToManyPBuffersResponse r) mutable
                {
                    promise.SetValue(std::move(r));
                };

                dbg->WriteBlocksToManyPBuffers(
                    0,   // VChunkIndex
                    coordinatorHost,
                    hosts,
                    100,   // lsn
                    TBlockRange64::WithLength(0, 3),
                    TDuration::Seconds(1),
                    guardedSglist,
                    CreateTraceId(),
                    cb);
                return future;
            });
        pendingWrite.GetValue(WaitTimeout).GetValue(WaitTimeout);

        // The coordinator's PBuffer DDisk must be first in the request.
        const auto& pbuffers = transportPtr->GetPBufferIds();
        const auto& sentIds = transportPtr->LastWriteToManyPBuffersDiskIds;
        UNIT_ASSERT_VALUES_EQUAL(3, sentIds.size());
        UNIT_ASSERT_VALUES_EQUAL(
            pbuffers[coordinatorHost].NodeId,
            sentIds[0].GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(
            pbuffers[coordinatorHost].PDiskId,
            sentIds[0].GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            pbuffers[coordinatorHost].DDiskSlotId,
            sentIds[0].GetDDiskSlotId());

        UNIT_ASSERT_VALUES_EQUAL(pbuffers[1].NodeId, sentIds[1].GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(pbuffers[1].PDiskId, sentIds[1].GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            pbuffers[1].DDiskSlotId,
            sentIds[1].GetDDiskSlotId());

        UNIT_ASSERT_VALUES_EQUAL(pbuffers[4].NodeId, sentIds[2].GetNodeId());
        UNIT_ASSERT_VALUES_EQUAL(pbuffers[4].PDiskId, sentIds[2].GetPDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            pbuffers[4].DDiskSlotId,
            sentIds[2].GetDDiskSlotId());
    }

    Y_UNIT_TEST_F(
        ShouldReplyForCoordinatorOnlyWhenNodeDisconnected,
        TDBGFixture)
    {
        const auto coordinatorHost = THostIndex(2);

        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->WriteToManyPBufferCoordinatorOnlyStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        TString pendingBuffer(DefaultBlockSize, 'p');
        TGuardedSgList guardedSglist = MakeSgList(pendingBuffer);
        THostMask hosts;
        hosts.Set(1);
        hosts.Set(coordinatorHost);
        hosts.Set(3);

        auto pendingWrite = RunOnExecutor(
            executor,
            [&]
            {
                NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse>
                    promise = NThreading::NewPromise<
                        TDBGWriteBlocksToManyPBuffersResponse>();
                auto future = promise.GetFuture();
                TDirectBlockGroup::TWriteBlocksToManyPBuffersCallback cb =
                    [promise = std::move(promise)]   //
                    (TDBGWriteBlocksToManyPBuffersResponse r) mutable
                {
                    promise.SetValue(std::move(r));
                };

                dbg->WriteBlocksToManyPBuffers(
                    0,   // VChunkIndex
                    coordinatorHost,
                    hosts,
                    100,   // lsn
                    TBlockRange64::WithLength(0, 3),
                    TDuration::Seconds(1),
                    guardedSglist,
                    CreateTraceId(),
                    cb);
                return future;
            });
        auto writeResponse =
            pendingWrite.GetValue(WaitTimeout).GetValue(WaitTimeout);

        // Only the coordinator host is present in the response with an error.
        UNIT_ASSERT_VALUES_EQUAL(1, writeResponse.Responses.size());
        UNIT_ASSERT_VALUES_EQUAL(
            coordinatorHost,
            writeResponse.Responses[0].HostIndex);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FAIL,
            writeResponse.Responses[0].Error.GetCode(),
            FormatError(writeResponse.Responses[0].Error));

        // Coordinator host: the WriteToManyPBuffers inflight is drained and an
        // error is counted.
        {
            const auto& hostStat =
                dbg->GetOracle()->GetHostStatistics(coordinatorHost);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::WriteToManyPBuffers));
            UNIT_ASSERT_VALUES_EQUAL(1, errorsInfo.ConsecutiveErrorCount);
        }
        // Non-coordinator hosts should not have errors or inflight changes.
        for (auto host: {THostIndex(1), THostIndex(3)}) {
            const auto& hostStat = dbg->GetOracle()->GetHostStatistics(host);
            const auto& errorsInfo = hostStat.GetErrorsInfo(TInstant::Now());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                hostStat.InflightCount(EOperation::WriteToManyPBuffers));
            UNIT_ASSERT_VALUES_EQUAL(0, errorsInfo.ConsecutiveErrorCount);
        }
    }

    // BLOCKED at Connect: the stale tablet must suicide, mark the
    // session terminally broken, reject the pending read, and never reconnect.
    Y_UNIT_TEST_F(ShouldSuicideOnBlockedConnect, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        // host[0] connect is deferred; hosts 1..4 connect immediately -> the
        // quorum (3 of 5) is reached without host[0].
        auto pendingConnect0 =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        // Snapshot connect attempts on host[0] before BLOCKED is delivered.
        const size_t connectsBefore =
            transportPtr
                ->GetConnectCredentials(EConnectionType::DDisk, ddisks[0])
                .size();

        // Read on the still-connecting host[0] suspends in WaitForSessionLock.
        const auto range = TBlockRange64::WithLength(0, 1);
        TString pendingBuffer(DefaultBlockSize, 'p');
        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(pendingBuffer),
                    NWilson::TTraceId());
            });
        auto inFlightRead = WaitFuture(executor, pendingRead, WaitTimeout);
        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(!inFlightRead.HasValue());

        // Resolve the connect with BLOCKED -> stale generation, suicide.
        pendingConnect0.SetValue(TStorageTransportMock::MakeConnectResult(
            1,
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED));

        auto response = WaitFuture(executor, inFlightRead, WaitTimeout);
        // The suspended read is rejected once the session becomes Broken.
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response.Error.GetCode());

        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(state.DDiskSessionBroken);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        UNIT_ASSERT_VALUES_EQUAL(1, service.BlockedGenerationCount);
        UNIT_ASSERT(service.LastBlockedReason.Contains("Connect"));
        UNIT_ASSERT(service.LastBlockedReason.Contains("BLOCKED"));

        // No reconnect was issued for host[0].
        const size_t connectsAfter =
            transportPtr
                ->GetConnectCredentials(EConnectionType::DDisk, ddisks[0])
                .size();
        UNIT_ASSERT_VALUES_EQUAL(connectsBefore, connectsAfter);
    }

    // BLOCKED on a regular DDisk write: the response is rejected and the
    // instance suicides.
    Y_UNIT_TEST_F(ShouldSuicideOnBlockedWrite, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->WriteToDDiskStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        const auto range = TBlockRange64::WithLength(0, 1);
        TString writeBuffer(DefaultBlockSize, 'w');
        auto pendingWrite = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->WriteBlocksToDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(writeBuffer),
                    NWilson::TTraceId());
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            GetResponse(pendingWrite).Error.GetCode());

        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(state.DDiskSessionBroken);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        UNIT_ASSERT_VALUES_EQUAL(1, service.BlockedGenerationCount);
        UNIT_ASSERT(service.LastBlockedReason.Contains("WriteToDDisk"));
    }

    // BLOCKED on a regular DDisk read: same terminal reaction as write.
    Y_UNIT_TEST_F(ShouldSuicideOnBlockedRead, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->ReadFromDDiskStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        const auto range = TBlockRange64::WithLength(0, 1);
        TString readBuffer(DefaultBlockSize, 'r');
        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(readBuffer),
                    NWilson::TTraceId());
            });
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            GetResponse(pendingRead).Error.GetCode());

        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(state.DDiskSessionBroken);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        UNIT_ASSERT_VALUES_EQUAL(1, service.BlockedGenerationCount);
        UNIT_ASSERT(service.LastBlockedReason.Contains("ReadFromDDisk"));
    }

    // BLOCKED on SyncWithPBuffer: the DDisk receiver is stale. All segments
    // are rejected and the suicide is attributed to the DDisk host.
    Y_UNIT_TEST_F(ShouldSuicideOnBlockedSyncWithPBuffer, TDBGFixture)
    {
        const auto pbufferHost = THostIndex(1);
        const auto ddiskHost = THostIndex(2);

        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->SyncWithPBufferStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        auto pendingFlush = RunOnExecutor(
            executor,
            [&]
            {
                TVector<TPBufferSegment> segments;
                segments.push_back(
                    TPBufferSegment(100, TBlockRange64::WithLength(0, 3)));

                return dbg->SyncWithPBuffer(
                    10,
                    pbufferHost,
                    ddiskHost,
                    segments,
                    NWilson::TTraceId());
            });
        auto flushResponse =
            pendingFlush.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1, flushResponse.Errors.size());
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, flushResponse.Errors[0].GetCode());

        const auto state =
            GetBlockedDetected(executor, dbg, ddiskHost, WaitTimeout);
        UNIT_ASSERT(state.DDiskSessionBroken);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        UNIT_ASSERT_VALUES_EQUAL(1, service.BlockedGenerationCount);
        UNIT_ASSERT(service.LastBlockedReason.Contains("SyncWithPBuffer"));
    }

    // Idempotency: several BLOCKED responses across hosts trigger the
    // suicide (OnBlockedGeneration) exactly once.
    Y_UNIT_TEST_F(ShouldTriggerSuicideOnceOnMultipleBlocked, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->WriteToDDiskStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        const auto range = TBlockRange64::WithLength(0, 1);
        for (THostIndex host: {THostIndex(0), THostIndex(1)}) {
            TString writeBuffer(DefaultBlockSize, 'w');
            auto pendingWrite = RunOnExecutor(
                executor,
                [&]
                {
                    return dbg->WriteBlocksToDDisk(
                        0,
                        host,
                        range,
                        MakeSgList(writeBuffer),
                        NWilson::TTraceId());
                });
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                GetResponse(pendingWrite).Error.GetCode());
        }

        // Barrier on the executor to make sure all callbacks have run.
        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(state.DDiskSessionBroken);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        UNIT_ASSERT_VALUES_EQUAL(1, service.BlockedGenerationCount);
    }

    // After BLOCKED no reconnect is issued even on an IC disconnect.
    Y_UNIT_TEST_F(ShouldNotReconnectAfterBlocked, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        auto pendingConnect0 =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        pendingConnect0.SetValue(TStorageTransportMock::MakeConnectResult(
            1,
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED));

        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(state.BlockedGenerationDetected);

        const size_t connectsBefore =
            transportPtr
                ->GetConnectCredentials(EConnectionType::DDisk, ddisks[0])
                .size();

        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            ddisks[0].NodeId);
        // Drain OnNodeDisconnected and any (suppressed) reconnect.
        DrainExecutor(executor);
        DrainExecutor(executor);

        const size_t connectsAfter =
            transportPtr
                ->GetConnectCredentials(EConnectionType::DDisk, ddisks[0])
                .size();
        UNIT_ASSERT_VALUES_EQUAL(connectsBefore, connectsAfter);
    }

    // A PBuffer error must NOT trigger the suicide: the BLOCKED check is
    // only wired into DDisk-addressed responses.
    Y_UNIT_TEST_F(ShouldNotSuicideOnPBufferError, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        transport->WriteToManyPBufferStatus =
            NKikimrBlobStorage::NDDisk::TReplyStatus::BLOCKED;
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        TString pendingBuffer(DefaultBlockSize, 'p');
        TGuardedSgList guardedSglist = MakeSgList(pendingBuffer);
        THostMask hosts;
        hosts.Set(1);
        hosts.Set(2);
        hosts.Set(3);

        auto pendingIndirectWrite = RunOnExecutor(
            executor,
            [&]
            {
                NThreading::TPromise<TDBGWriteBlocksToManyPBuffersResponse>
                    promise = NThreading::NewPromise<
                        TDBGWriteBlocksToManyPBuffersResponse>();
                auto future = promise.GetFuture();
                TDirectBlockGroup::TWriteBlocksToManyPBuffersCallback cb =
                    [promise = std::move(promise)]   //
                    (TDBGWriteBlocksToManyPBuffersResponse r) mutable
                {
                    promise.SetValue(std::move(r));
                };

                dbg->WriteBlocksToManyPBuffers(
                    0,
                    2,
                    hosts,
                    100,
                    TBlockRange64::WithLength(0, 3),
                    TDuration::Seconds(1),
                    guardedSglist,
                    NWilson::TTraceId(),
                    cb);
                return future;
            });
        auto writeResponse =
            pendingIndirectWrite.GetValue(WaitTimeout).GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(3, writeResponse.Responses.size());
        for (const auto& response: writeResponse.Responses) {
            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
        }

        const auto state = GetBlockedDetected(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT(!state.DDiskSessionBroken);
        UNIT_ASSERT(!state.BlockedGenerationDetected);
        UNIT_ASSERT_VALUES_EQUAL(0, service.BlockedGenerationCount);
    }

    // QueryAddHost() routes an add-host request to the partition-direct
    // service, tagged with this DBG's index.
    Y_UNIT_TEST_F(ShouldQueryAddHostThroughService, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto dbg = MakeDirectBlockGroup(
            executor,
            std::make_unique<TStorageTransportMock>());

        auto initialReady = RunAndGetInitialReady(dbg);
        WaitReady(executor, initialReady);

        auto& service = *Services.back();
        UNIT_ASSERT_VALUES_EQUAL(0u, service.AddHostRequests.size());

        RunOnExecutor(
            executor,
            [&]
            {
                dbg->QueryAddHost();
                return true;
            })
            .GetValue(WaitTimeout);

        UNIT_ASSERT_VALUES_EQUAL(1u, service.AddHostRequests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<size_t>(0),
            service.AddHostRequests[0]);
    }

    // On restart a DBG comes up with the committed connection count (here N+1).
    // The Oracle is born at that count in the constructor, and a vchunk whose
    // persisted config still lags at N is caught up by the DBG the moment it
    // registers - the same OnHostAppended path a live AddHost uses, with no
    // partition involved.
    Y_UNIT_TEST_F(ShouldCatchUpHostsOnStartup, TDBGFixture)
    {
        constexpr ui32 grownHostCount = DirectBlockGroupHostCount + 1;
        constexpr ui64 vChunkSize = RegionSize / DirectBlockGroupsCount;

        auto executor = MakeExecutor();

        // The DBG comes up already grown to N+1 connections.
        auto dbg = MakeDirectBlockGroup(
            executor,
            std::make_unique<TStorageTransportMock>(),
            MakeDDiskIds(100, grownHostCount),
            MakeDDiskIds(100 + grownHostCount, grownHostCount));

        auto initialReady = RunAndGetInitialReady(dbg);
        WaitReady(executor, initialReady);

        // A vchunk that still only knows the pre-add host count.
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(
            new ::NMonitoring::TDynamicCounters());
        auto vchunk = std::make_shared<TVChunk>(
            Runtime->GetActorSystem(0),
            Services.back().get(),
            TVChunkConfig::MakeDefault(
                100,
                DirectBlockGroupHostCount,
                DefaultPrimaryCount),
            dbg,
            3,
            vChunkSize,
            counters);

        TString oracleDump;
        TString dumpBefore;
        TString dumpAfter;
        RunOnExecutor(
            executor,
            [&]
            {
                oracleDump = dbg->GetOracle()->Dump();
                dumpBefore = vchunk->DebugPrintDirtyMap();
                dbg->Register(vchunk);
                dumpAfter = vchunk->DebugPrintDirtyMap();
                return true;
            })
            .GetValue(WaitTimeout);

        // The Oracle is born at the connection count - the grown slot H5 is
        // already present.
        UNIT_ASSERT_STRING_CONTAINS(oracleDump, "H5");

        // The grown host slot (H5) is absent in the vchunk before registering
        // and present after - the DBG caught it up at registration.
        UNIT_ASSERT_C(
            dumpBefore.find("H5-") == TString::npos,
            "unexpected H5 before register:\n" + dumpBefore);
        UNIT_ASSERT_STRING_CONTAINS(dumpAfter, "H5-");
    }
}

Y_UNIT_TEST_SUITE(TDDiskSessionSeqNoTest)
{
    Y_UNIT_TEST_F(ShouldHaveZeroConfirmedSeqNoBeforeConnect, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        const auto& ddisks = transport->GetDDiskIds();

        // Defer every DDisk connect so no session gets confirmed.
        TVector<TStorageTransportMock::TConnectPromise> connectPromises;
        for (const auto& ddiskId: ddisks) {
            connectPromises.push_back(
                transport->SetPendingConnect(EConnectionType::DDisk, ddiskId));
        }

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        const auto values = ReadAllDDiskSeqNos(executor, dbg, WaitTimeout);
        for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, values[i]);
        }
    }

    Y_UNIT_TEST_F(ShouldSeqNoOnInitialConnects, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        const auto& pbuffers = transportPtr->GetPBufferIds();

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        WaitReady(initialReady);

        // DDisks
        for (const auto& ddiskId: ddisks) {
            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::DDisk,
                ddiskId);
            UNIT_ASSERT_VALUES_EQUAL(1, credentials.size());
            UNIT_ASSERT_VALUES_EQUAL(1, credentials[0].DDiskSessionSeqNo);
        }

        // PBuffers
        for (const auto& pbufferId: pbuffers) {
            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::PBuffer,
                pbufferId);
            UNIT_ASSERT_VALUES_EQUAL(1, credentials.size());
            UNIT_ASSERT_VALUES_EQUAL(0, credentials[0].DDiskSessionSeqNo);
        }
    }

    // After the first Connect every DDisk has seq_no=1 and credentials carry
    // seq_no=1. After a disconnect + reconnect of one DDisk its seq_no becomes
    // 2, while the others stay at 1 (independent per-DDisk counters).
    Y_UNIT_TEST_F(
        ShouldConfirmSeqNoAfterInitialConnectAndReconnect,
        TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg, false);
        WaitReady(initialReady);

        // All sessions confirmed at seq_no=1.
        for (const auto seqNo: ReadAllDDiskSeqNos(executor, dbg, WaitTimeout)) {
            UNIT_ASSERT_VALUES_EQUAL(1, seqNo);
        }

        // Defer the reconnect of DDisk[0].
        auto reconnectPromise =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            ddisks[0].NodeId);
        // drain OnNodeDisconnected and queue reconnect
        DrainExecutor(executor);
        // drain reconnect
        DrainExecutor(executor);

        // The reconnect Connect already carries seq_no=2.
        const auto credentials = transportPtr->GetConnectCredentials(
            EConnectionType::DDisk,
            ddisks[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, credentials.back().DDiskSessionSeqNo);

        reconnectPromise.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        const auto values = ReadAllDDiskSeqNos(executor, dbg, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, values[0]);
        for (size_t i = 1; i < DirectBlockGroupHostCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(1, values[i]);
        }
    }

    // A stale connect response (seq_no <= ConfirmedSessionSeqNo) is ignored and
    // does not roll back the confirmed seq_no.
    Y_UNIT_TEST_F(ShouldIgnoreStaleConnectResponse, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        // seq_no=1 on DDisk[0] is in flight.
        auto firstConnect =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg, false);
        DrainExecutor(executor);

        // seq_no=2 on DDisk[0] is in flight.
        auto secondConnect =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            ddisks[0].NodeId);
        DrainExecutor(executor);

        // Resolve the new (seq_no=2) connect first.
        secondConnect.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        auto afterNew = GetDDiskSessionSeqNo(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, afterNew);

        // resolve the stale connect. It must be ignored.
        firstConnect.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        auto afterStale = GetDDiskSessionSeqNo(executor, dbg, 0, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(2, afterStale);
    }
}

Y_UNIT_TEST_SUITE(TSessionsWithRealTransport)
{
    Y_UNIT_TEST_F(ShouldCancelSessionWaitersOnDisconnect, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        // DDisk[0] stays pending; hosts 1..4 connect immediately -> quorum 4/5.
        transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);
        WaitReady(executor, initialReady);

        const auto range = TBlockRange64::WithLength(0, 1);
        TString buffer(DefaultBlockSize, 'r');

        // The read on host 0 suspends inside ReadBlocksFromDDisk.
        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(buffer),
                    CreateTraceId());
            });
        auto inFlightRead = WaitFuture(executor, pendingRead, WaitTimeout);
        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(!inFlightRead.HasValue());

        // The disconnect resets the session: ResetSession wakes the waiter with
        // an error.
        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            transportPtr->GetNodeId());

        auto response = WaitFuture(executor, inFlightRead, WaitTimeout);
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
    }

    Y_UNIT_TEST_F(ShouldCancelActiveRequestsOnDisconnect, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);
        WaitReady(executor, initialReady);

        // DDisk[0] no longer answers reads: the read future stays pending.
        transportPtr->SetPendingReadFromDDisk(
            EConnectionType::DDisk,
            ddisks[0]);

        const auto range = TBlockRange64::WithLength(0, 1);
        TString buffer(DefaultBlockSize, 'r');

        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(buffer),
                    CreateTraceId());
            });

        // The session is already established, so the read does not suspend.
        auto inFlightRead = WaitFuture(executor, pendingRead, WaitTimeout);
        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(!inFlightRead.HasValue());

        // The disconnect rejects the in-flight read with a "Session broken"
        // error.
        transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);
        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            transportPtr->GetNodeId());

        auto response = WaitFuture(executor, inFlightRead, WaitTimeout);
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
