#include "direct_block_group_test_fixture.h"

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

// Default time Settle() pumps the runtime + executor to let in-flight async
// work settle when there is no specific condition to wait for.
constexpr auto SettleDuration = TDuration::MilliSeconds(200);

using EConnectionType = NTransport::THostConnection::EConnectionType;
using TStorageTransportMock = NTransport::TStorageTransportMock;
using TICStorageTransportTestAdapter =
    NTransport::NTestLib::TICStorageTransportTestAdapter;

////////////////////////////////////////////////////////////////////////////////

TGuardedSgList MakeSgList(TString& buffer)
{
    return TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});
}

// Unwraps a future-of-future (e.g. the value returned by ReadBlocksFromDDisk,
// where the outer future resolves once the request is issued and the inner one
// once the response arrives) into the final response.
template <typename T>
T GetResponse(const TFuture<TFuture<T>>& outer, TDuration timeout = WaitTimeout)
{
    return outer.GetValue(timeout).GetValue(timeout);
}

// Reads GetDDiskSessionSeqNo() for every host on the executor thread.
TVector<ui64> ReadAllDDiskSeqNos(
    const TExecutorPtr& executor,
    const std::shared_ptr<TDirectBlockGroup>& dbg)
{
    return RunOnExecutor(
               executor,
               [&]
               {
                   TVector<ui64> result;
                   for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
                       result.push_back(dbg->GetDDiskSessionSeqNo(i));
                   }
                   return result;
               })
        .GetValue(WaitTimeout);
}

// Resolves pending connect promises in the half-open range [from, to) with a
// successful result.
void ResolveConnects(
    TVector<TStorageTransportMock::TConnectPromise>& promises,
    size_t from,
    size_t to)
{
    for (size_t i = from; i < to; ++i) {
        promises[i].SetValue(TStorageTransportMock::MakeConnectResult());
    }
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
                        NWilson::TTraceId());
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
                        NWilson::TTraceId());
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
                    NWilson::TTraceId());
            });
        DrainExecutor(executor);
        UNIT_ASSERT(!pendingRead.HasValue());

        // Establishing the session unblocks the read.
        pendingHost3.SetValue(TStorageTransportMock::MakeConnectResult());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            GetResponse(pendingRead).Error.GetCode());
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
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDDiskSessionSeqNoTest)
{
    Y_UNIT_TEST_F(ShouldSeqNo1OnInitialConnectToDDisk, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& ddisks = transportPtr->GetDDiskIds();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        WaitReady(initialReady);

        for (const auto& ddiskId: ddisks) {
            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::DDisk,
                ddiskId);
            UNIT_ASSERT_VALUES_EQUAL(1, credentials.size());
            UNIT_ASSERT_VALUES_EQUAL(1, credentials[0].DDiskSessionSeqNo);
        }
    }

    Y_UNIT_TEST_F(ShouldSeqNo0OnInitialConnectToPBuffer, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto& pbuffers = transportPtr->GetPBufferIds();
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);

        WaitReady(initialReady);

        for (const auto& pbufferId: pbuffers) {
            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::PBuffer,
                pbufferId);
            UNIT_ASSERT_VALUES_EQUAL(1, credentials.size());
            UNIT_ASSERT_VALUES_EQUAL(0, credentials[0].DDiskSessionSeqNo);
        }
    }

    // ConfirmedSessionSeqNo is initialized to zero before the first Connect is
    // answered.
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

        const auto values = ReadAllDDiskSeqNos(executor, dbg);
        for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, values[i]);
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
        auto initialReady = RunAndGetInitialReady(dbg);
        WaitReady(initialReady);

        // All sessions confirmed at seq_no=1.
        for (const auto seqNo: ReadAllDDiskSeqNos(executor, dbg)) {
            UNIT_ASSERT_VALUES_EQUAL(1, seqNo);
        }

        // Defer the reconnect of DDisk[0].
        auto reconnectPromise =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            ddisks[0].NodeId);
        DrainExecutor(executor);

        // The reconnect Connect already carries seq_no=2.
        const auto credentials = transportPtr->GetConnectCredentials(
            EConnectionType::DDisk,
            ddisks[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, credentials.back().DDiskSessionSeqNo);

        reconnectPromise.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        const auto values = ReadAllDDiskSeqNos(executor, dbg);
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
        // First Connect (seq_no=1) on DDisk[0] is in flight.
        auto firstConnect =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));
        auto initialReady = RunAndGetInitialReady(dbg);
        DrainExecutor(executor);

        // Register the reconnect (seq_no=2) pending Connect; SetPendingConnect
        // overwrites the entry, so the second Connect future is captured.
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

        auto afterNew = RunOnExecutor(
            executor,
            [&] { return dbg->GetDDiskSessionSeqNo(0); });
        UNIT_ASSERT_VALUES_EQUAL(2, afterNew.GetValue(WaitTimeout));

        // Now resolve the stale (seq_no=1) connect. It must be ignored.
        firstConnect.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        auto afterStale = RunOnExecutor(
            executor,
            [&] { return dbg->GetDDiskSessionSeqNo(0); });
        UNIT_ASSERT_VALUES_EQUAL(2, afterStale.GetValue(WaitTimeout));
    }
}

Y_UNIT_TEST_SUITE(TSessionsWithRealTransport)
{
    // A read coroutine waiting for the session lock must be released with an
    // error (not hang forever) after a disconnect resets the session.
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

        // The read on host 0 suspends inside WaitForSessionLock.
        auto pendingRead = RunOnExecutor(
            executor,
            [&]
            {
                return dbg->ReadBlocksFromDDisk(
                    0,
                    0,
                    range,
                    MakeSgList(buffer),
                    NWilson::TTraceId());
            });
        DoAllExecutorAndRuntimeWork(executor, SettleDuration);
        UNIT_ASSERT(!pendingRead.HasValue());

        // The disconnect resets the session: ResetSession wakes the waiter with
        // an error.
        transportPtr->FireDisconnect(
            EConnectionType::DDisk,
            ddisks[0],
            transportPtr->GetNodeId());

        auto innerRead = WaitFuture(executor, pendingRead, WaitTimeout);
        UNIT_ASSERT(pendingRead.HasValue());
        auto response = WaitFuture(executor, innerRead, WaitTimeout);
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
    }

    // A read request already sent to the DDisk (session established, request in
    // flight) is rejected with an error when the node disconnects.
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
                    NWilson::TTraceId());
            });

        // The session is already established, so the read does not suspend in
        // WaitForSessionLock: the outer future resolves immediately, carrying
        // the still-pending in-flight read future.
        auto inFlightRead = WaitFuture(executor, pendingRead, WaitTimeout);
        DoAllExecutorAndRuntimeWork(executor, SettleDuration);
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
