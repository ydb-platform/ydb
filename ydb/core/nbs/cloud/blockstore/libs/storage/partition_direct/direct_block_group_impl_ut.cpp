#include "direct_block_group_impl.h"

#include "ut/storage_transport_mock.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service_mock.h>

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
        TVector<TStorageTransportMock::TConnectPromise> connectPromises;
        for (const auto& ddiskId: ddisks) {
            connectPromises.push_back(transportPtr->SetPendingConnect(
                EConnectionType::DDisk,
                ddiskId));
        }
        connectPromises.push_back(
            transportPtr->SetPendingConnect(EConnectionType::DDisk, ddisks[0]));

        auto dbg = MakeDirectBlockGroup(executor, std::move(transport));

        auto initialReady = dbg->GetInitialReadyFuture();

        TPartitionDirectServiceMock service(true);
        dbg->Run(&service);

        // Establish-connections has run, but no DDisk session is locked yet.
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // Resolve two sessions: still below the quorum of three.
        connectPromises[0].SetValue(TStorageTransportMock::MakeConnectResult());
        connectPromises[1].SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);
        UNIT_ASSERT(!initialReady.HasValue());

        // The third locked session reaches the quorum -> signal fires.
        connectPromises[2].SetValue(TStorageTransportMock::MakeConnectResult());
        DrainExecutor(executor);
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(!initialReady.HasValue());

        // Last PBuffer connected
        connectPromises[connectPromises.size() - 1].SetValue(
            TStorageTransportMock::MakeConnectResult());
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        // Locking the remaining sessions must not re-signal (one-shot promise;
        // a second SetValue would abort).
        connectPromises[3].SetValue(TStorageTransportMock::MakeConnectResult());
        connectPromises[4].SetValue(TStorageTransportMock::MakeConnectResult());
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

        auto initialReady = dbg->GetInitialReadyFuture();

        TPartitionDirectServiceMock service(true);
        dbg->Run(&service);

        // Three immediate sessions -> quorum reached.
        initialReady.Wait(WaitTimeout);
        UNIT_ASSERT(initialReady.HasValue());

        const auto range = TBlockRange64::WithLength(0, 1);

        // Read from a locked host completes right away.
        TString readyBuffer(DefaultBlockSize, 'r');
        auto readyRead = InvokeOnExecutor(
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
        auto readyWrite = InvokeOnExecutor(
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
        auto pendingRead = InvokeOnExecutor(
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

        // Mirror fast_path_service.cpp: WaitAll over per-DBG initial-ready.
        TVector<TFuture<void>> initialReadyFutures{
            dbgA->GetInitialReadyFuture(),
            dbgB->GetInitialReadyFuture()};
        auto allReady = NThreading::WaitAll(initialReadyFutures);

        TPartitionDirectServiceMock serviceA(true);
        TPartitionDirectServiceMock serviceB(true);
        dbgA->Run(&serviceA);
        dbgB->Run(&serviceB);

        // DBG A is ready; DBG B is not -> the aggregate gate is not resolved.
        dbgA->GetInitialReadyFuture().Wait(WaitTimeout);
        UNIT_ASSERT(dbgA->GetInitialReadyFuture().HasValue());
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
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
