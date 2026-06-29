#include "direct_block_group_impl.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/storage_transport_mock.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor_ut.h>

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <ydb/library/services/services.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <vector>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitTimeout = TDuration::Seconds(10);

using EConnectionType = NTransport::THostConnection::EConnectionType;
using TStorageTransportMock = NTransport::TStorageTransportMock;
using TDDiskId = NBsController::TDDiskId;

////////////////////////////////////////////////////////////////////////////////

TVector<TDDiskId> MakeDDiskIds(ui32 baseNodeId)
{
    TVector<TDDiskId> ids;
    ids.reserve(DirectBlockGroupHostCount);
    for (ui32 i = 0; i < DirectBlockGroupHostCount; ++i) {
        ids.emplace_back(baseNodeId + i, 1, i);
    }
    return ids;
}

TStorageConfigPtr MakeStorageConfig()
{
    NProto::TStorageServiceConfig rawConfig;
    return std::make_shared<TStorageConfig>(rawConfig);
}

////////////////////////////////////////////////////////////////////////////////

struct TDBGFixture: public NUnitTest::TBaseFixture
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    TVector<TExecutorPtr> Executors;

    void SetUp(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);
        Runtime = std::make_unique<NActors::TTestActorRuntime>();
        Runtime->Initialize(TTestActorRuntime::TEgg{
            .App0 = new TAppData(
                0,
                0,
                0,
                0,
                {},
                nullptr,
                nullptr,
                nullptr,
                nullptr),
            .Opaque = nullptr,
            .KeyConfigGenerator = nullptr,
            .Icb = {},
            .Dcb = {}});
        Runtime->SetLogPriority(
            NKikimrServices::NBS_PARTITION,
            NLog::PRI_DEBUG);
    }

    void TearDown(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);
        for (const auto& executor: Executors) {
            executor->Stop();
        }
        Executors.clear();
    }

    TExecutorPtr MakeExecutor()
    {
        auto executor = TExecutor::Create("DBG_TEST");
        executor->Start();
        Executors.push_back(executor);
        return executor;
    }

    std::shared_ptr<TDirectBlockGroup> MakeDirectBlockGroup(
        const TExecutorPtr& executor,
        std::unique_ptr<TStorageTransportMock> transport,
        ui32 baseNodeId = 100)
    {
        return std::make_shared<TDirectBlockGroup>(
            Runtime->GetActorSystem(0),
            MakeStorageConfig(),
            executor,
            "disk-1",
            1,
            1,
            0,
            MakeDDiskIds(baseNodeId),
            MakeDDiskIds(baseNodeId + DirectBlockGroupHostCount),
            std::move(transport));
    }
};

TGuardedSgList MakeSgList(TString& buffer)
{
    return TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDirectBlockGroupTest)
{
    // The initial-ready signal fires exactly once, only after the locked
    // quorum (3 of 5 DDisk sessions) is reached.
    Y_UNIT_TEST_F(ShouldSignalInitialReadyOnceLockedQuorumReached, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);

        // All DDisk connects are deferred -> the sessions stay NotLocked until
        // the test resolves them.
        TVector<TStorageTransportMock::TConnectPromise> connectDDiskPromises;
        for (const auto& ddiskId: ddisks) {
            connectDDiskPromises.push_back(transportPtr->SetPendingConnect(
                EConnectionType::DDisk,
                ddiskId));
        }

        const auto pbuffers = MakeDDiskIds(100 + DirectBlockGroupHostCount);
        TVector<TStorageTransportMock::TConnectPromise> connectPBufferPromises;
        for (size_t i = 2; i < pbuffers.size(); ++i) {
            connectPBufferPromises.push_back(transportPtr->SetPendingConnect(
                EConnectionType::PBuffer,
                pbuffers[i]));
        }

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        // Establish-connections has run, but no DDisk session is locked yet.
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // Resolve 3 ddisk sessions: still don't have pbuffer's quorum.
        connectDDiskPromises[0].SetValue(
            TStorageTransportMock::MakeConnectResult());
        connectDDiskPromises[1].SetValue(
            TStorageTransportMock::MakeConnectResult());
        connectDDiskPromises[2].SetValue(
            TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // The third PBuffer connected
        connectPBufferPromises[0].SetValue(
            TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);
        UNIT_ASSERT(initialReady.HasValue());
    }

    // With a 3-of-5 quorum, reads/writes to a locked host pass through,
    // while a request to a still-connecting host is suspended until its session
    // is established.
    Y_UNIT_TEST_F(ShouldBlockDDiskIoUntilSessionEstablished, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);

        // Hosts 0..2 connect immediately (default) and form the quorum; hosts
        // 3 and 4 stay pending.
        auto pendingHost3 =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[3]);
        auto pendingHost4 =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[4]);
        Y_UNUSED(pendingHost4);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        // Three immediate sessions -> quorum reached.
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        const auto range = TBlockRange64::WithLength(0, 1);

        // Read from a locked host completes right away.
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
        auto readyReadResponse =
            readyRead.GetValue(WaitTimeout).GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, readyReadResponse.Error.GetCode());

        // Write to a locked host completes right away.
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
        auto readyWriteResponse =
            readyWrite.GetValue(WaitTimeout).GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, readyWriteResponse.Error.GetCode());

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
        auto pendingReadResponse =
            pendingRead.GetValue(WaitTimeout).GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, pendingReadResponse.Error.GetCode());
    }

    // The tablet-wide "all DBGs ready" gate (WaitAll over per-DBG
    // initial-ready futures) stays unresolved while any single DBG is below its
    // quorum, and resolves once every DBG reaches it.
    Y_UNIT_TEST_F(ShouldWaitAllDBGsInitiallyReady, TDBGFixture)
    {
        // DBG A: every DDisk session connects immediately -> ready after Run.
        auto executorA = MakeExecutor();
        auto dbgA = MakeDirectBlockGroup(
            executorA,
            std::make_unique<TStorageTransportMock>(),
            100);

        // DBG B: every DDisk session is deferred -> not ready yet.
        auto executorB = MakeExecutor();
        auto transportB = std::make_unique<TStorageTransportMock>();
        auto* transportBPtr = transportB.get();
        const auto ddisksB = MakeDDiskIds(200);
        TVector<TStorageTransportMock::TConnectPromise> connectPromisesB;
        for (const auto& ddiskId: ddisksB) {
            connectPromisesB.push_back(transportBPtr->SetPendingConnect(
                EConnectionType::DDisk,
                ddiskId));
        }
        auto dbgB = MakeDirectBlockGroup(executorB, std::move(transportB), 200);

        // Mirror fast_path_service.cpp: WaitAll over per-DBG initial-ready
        // futures returned by Run().
        TPartitionDirectServiceMock serviceA(true);
        TPartitionDirectServiceMock serviceB(true);
        auto initialReadyA = dbgA->Run(&serviceA);
        auto initialReadyB = dbgB->Run(&serviceB);

        TVector<TFuture<void>> initialReadyFutures{
            initialReadyA,
            initialReadyB};
        auto allReady = NThreading::WaitAll(initialReadyFutures);

        // DBG A is ready; DBG B is not -> the aggregate gate is not resolved.
        initialReadyA.Wait(WaitTimeout);
        UNIT_ASSERT(initialReadyA.HasValue());
        DrainExecutor(executorB);
        UNIT_ASSERT(!allReady.HasValue());

        // Bring DBG B to its quorum (3 of 5).
        connectPromisesB[0].SetValue(
            TStorageTransportMock::MakeConnectResult());
        connectPromisesB[1].SetValue(
            TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executorB);
        UNIT_ASSERT(!allReady.HasValue());

        connectPromisesB[2].SetValue(
            TStorageTransportMock::MakeConnectResult());

        // Now every DBG is ready -> the aggregate gate resolves.
        allReady.Wait(WaitTimeout);
        UNIT_ASSERT(allReady.HasValue());
    }

    Y_UNIT_TEST_F(ShouldSeqNo1OnInitialConnectToDDisk, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());
        DrainExecutor(executor);

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

        const auto pbuffers = MakeDDiskIds(100 + DirectBlockGroupHostCount);
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());
        DrainExecutor(executor);

        for (const auto& pbufferId: pbuffers) {
            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::PBuffer,
                pbufferId);
            UNIT_ASSERT_VALUES_EQUAL(1, credentials.size());
            UNIT_ASSERT_VALUES_EQUAL(0, credentials[0].DDiskSessionSeqNo);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDDiskSessionSeqNoTest)
{
    // ConfirmedSessionSeqNo is initialized to zero before the first Connect is
    // answered.
    Y_UNIT_TEST_F(ShouldHaveZeroConfirmedSeqNoBeforeConnect, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);

        // Defer every DDisk connect so no session gets confirmed.
        TVector<TStorageTransportMock::TConnectPromise> connectPromises;
        for (const auto& ddiskId: ddisks) {
            connectPromises.push_back(transportPtr->SetPendingConnect(
                EConnectionType::DDisk,
                ddiskId));
        }

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        auto seqNos = RunOnExecutor(
            executor,
            [&]
            {
                TVector<ui64> result;
                for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
                    result.push_back(dbg->GetDDiskSessionSeqNo(i));
                }
                return result;
            });
        const auto values = seqNos.GetValue(WaitTimeout);
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

        const auto ddisks = MakeDDiskIds(100);
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());
        DrainExecutor(executor);

        // All sessions confirmed at seq_no=1.
        auto seqNosAfterConnect = RunOnExecutor(
            executor,
            [&]
            {
                TVector<ui64> result;
                for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
                    result.push_back(dbg->GetDDiskSessionSeqNo(i));
                }
                return result;
            });
        for (const auto seqNo: seqNosAfterConnect.GetValue(WaitTimeout)) {
            UNIT_ASSERT_VALUES_EQUAL(1, seqNo);
        }

        // Defer the reconnect of DDisk[0].
        auto reconnectPromise =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        transportPtr->FireDisconnect(EConnectionType::DDisk, ddisks[0], 100);
        DrainExecutor(executor);

        // The reconnect Connect already carries seq_no=2.
        const auto credentials = transportPtr->GetConnectCredentials(
            EConnectionType::DDisk,
            ddisks[0]);
        UNIT_ASSERT_VALUES_EQUAL(2, credentials.back().DDiskSessionSeqNo);

        reconnectPromise.SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);

        auto finalSeqNos = RunOnExecutor(
            executor,
            [&]
            {
                TVector<ui64> result;
                for (size_t i = 0; i < DirectBlockGroupHostCount; ++i) {
                    result.push_back(dbg->GetDDiskSessionSeqNo(i));
                }
                return result;
            });
        const auto values = finalSeqNos.GetValue(WaitTimeout);
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

        const auto ddisks = MakeDDiskIds(100);

        // First Connect (seq_no=1) on DDisk[0] is in flight.
        auto firstConnect =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);
        DrainExecutor(executor);

        // Register the reconnect (seq_no=2) pending Connect; SetPendingConnect
        // overwrites the entry, so the second Connect future is captured.
        auto secondConnect =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        transportPtr->FireDisconnect(EConnectionType::DDisk, ddisks[0], 100);
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

    // TODO delete me?
    // seq_no grows monotonically across N consecutive reconnects.
    Y_UNIT_TEST_F(ShouldIncrementSeqNoMonotonically, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());
        DrainExecutor(executor);

        // Three reconnects -> credentials seq_no should be 1, 2, 3, 4.
        for (ui64 expectedSeqNo = 2; expectedSeqNo <= 4; ++expectedSeqNo) {
            auto reconnectPromise = transportPtr->SetPendingConnect(
                EConnectionType::DDisk,
                ddisks[0]);
            transportPtr->FireDisconnect(
                EConnectionType::DDisk,
                ddisks[0],
                100);
            DrainExecutor(executor);

            const auto credentials = transportPtr->GetConnectCredentials(
                EConnectionType::DDisk,
                ddisks[0]);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedSeqNo,
                credentials.back().DDiskSessionSeqNo);

            reconnectPromise.SetValue(
                TStorageTransportMock::MakeConnectResult());
            DrainExecutor(executor);
        }

        auto finalSeqNo = RunOnExecutor(
            executor,
            [&] { return dbg->GetDDiskSessionSeqNo(0); });
        UNIT_ASSERT_VALUES_EQUAL(4, finalSeqNo.GetValue(WaitTimeout));
    }

    // A read coroutine waiting for the session lock must be released with an
    // error (not hang forever) after a disconnect resets the session.
    Y_UNIT_TEST_F(ShouldCancelSessionWaitersOnDisconnect, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);

        // DDisk[0] stays pending; hosts 1..4 connect immediately -> quorum 4/5.
        auto pendingHost0 =
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

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
        DrainExecutor(executor);
        UNIT_ASSERT(!pendingRead.HasValue());

        // The disconnect resets the session: ResetSession wakes the waiter with
        // an error.
        transportPtr->FireDisconnect(EConnectionType::DDisk, ddisks[0], 100);

        auto response = pendingRead.GetValue(WaitTimeout).GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
    }

    // A read request already sent to the DDisk (session established, request in
    // flight) is rejected with an error when the node disconnects.
    Y_UNIT_TEST_F(ShouldCancelActiveRequestsOnDisconnect, TDBGFixture)
    {
        auto executor = MakeExecutor();

        auto transport = std::make_unique<TStorageTransportMock>();
        auto* transportPtr = transport.get();

        const auto ddisks = MakeDDiskIds(100);
        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        TPartitionDirectServiceMock service(true);
        auto initialReady = dbg->Run(&service);

        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());
        DrainExecutor(executor);

        // DDisk[0] no longer answers reads: the read future stays pending.
        auto pendingReadFromDDisk = transportPtr->SetPendingReadFromDDisk(
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
        DrainExecutor(executor);

        // The session is already established, so the read does not suspend in
        // WaitForSessionLock: the outer future resolves immediately, carrying
        // the still-pending in-flight read future.
        UNIT_ASSERT(pendingRead.HasValue());
        auto inFlightRead = pendingRead.GetValue();
        UNIT_ASSERT(!inFlightRead.HasValue());

        // The disconnect rejects the in-flight read with a "Session broken"
        // error.
        transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]);
        transportPtr->FireDisconnect(EConnectionType::DDisk, ddisks[0], 100);

        auto response = inFlightRead.GetValue(WaitTimeout);
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response.Error.GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
