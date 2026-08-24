#include "direct_block_group_test_fixture.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/fake_direct_session.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/storage_transport/testlib/ic_storage_transport_test_adapter.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error_utils.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;
using namespace NTransport;
using namespace NTransport::NTestLib;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitTimeout = TDuration::Seconds(10);

using EConnectionType = THostConnection::EConnectionType;

THostConnection MakeDDiskConnection(
    const NBsController::TDDiskId& ddiskId,
    std::optional<ui64> guid = std::nullopt)
{
    return THostConnection{
        .ConnectionType = EConnectionType::DDisk,
        .DDiskId = ddiskId,
        .Credentials = NDDisk::TQueryCredentials::ToDDisk(
            /*tabletId=*/100,
            /*generation=*/1,
            /*sessionSeqNo=*/1,
            guid,
            /*directBlockGroupIndex=*/0)};
}

THostConnection MakePBufferConnection(
    const NBsController::TDDiskId& ddiskId,
    std::optional<ui64> guid = std::nullopt)
{
    return THostConnection{
        .ConnectionType = EConnectionType::PBuffer,
        .DDiskId = ddiskId,
        .Credentials = NDDisk::TQueryCredentials::ToPersistentBuffer(
            /*tabletId=*/100,
            /*generation=*/1,
            guid,
            /*directBlockGroupIndex=*/0)};
}

TGuardedSgList MakeSgList(TString& buffer)
{
    return TGuardedSgList(TSgList{TBlockDataRef{buffer.data(), buffer.size()}});
}

[[nodiscard]] NKikimrBlobStorage::NDDisk::TDDiskId ToProto(
    const NBsController::TDDiskId& ddiskId)
{
    NKikimrBlobStorage::NDDisk::TDDiskId pb;
    ddiskId.Serialize(&pb);
    return pb;
}

[[nodiscard]] TVector<NKikimrBlobStorage::NDDisk::TDDiskId> ToProto(
    const TVector<NBsController::TDDiskId>& ids)
{
    TVector<NKikimrBlobStorage::NDDisk::TDDiskId> result;
    result.reserve(ids.size());
    for (const auto& id: ids) {
        result.push_back(ToProto(id));
    }
    return result;
}

void CheckDirectWriteChecksums(TDBGFixture& fixture, bool directSession)
{
    auto executor = fixture.MakeExecutor();
    auto transport =
        std::make_unique<TICStorageTransportTestAdapter>(fixture.Runtime.get());
    if (directSession) {
        transport->EnableFakeDirectSession();
    }

    const auto& ddiskId = transport->GetDDiskIds()[0];
    auto connect = transport->Connect(MakeDDiskConnection(ddiskId));
    fixture.WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
    UNIT_ASSERT(
        connect.ConnectFuture.GetValueSync().GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

    auto connection = MakeDDiskConnection(
        ddiskId,
        connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

    TString writeBuf =
        TString(DefaultBlockSize, 'A') + TString(DefaultBlockSize, 'B');
    const auto expectedChecksums =
        NDDisk::CalculatePayloadChecksums(TRope(writeBuf));

    ui32 writeRequests = 0;
    TString observedPayload;
    TVector<ui64> observedChecksums;
    fixture.Runtime->SetObserverFunc(
        [&](TAutoPtr<NActors::IEventHandle>& ev)
        {
            if (ev->GetTypeRewrite() == NDDisk::TEvWrite::EventType) {
                ++writeRequests;
                auto* msg = ev->Get<NDDisk::TEvWrite>();
                UNIT_ASSERT_VALUES_EQUAL(msg->GetPayloadCount(), 1u);
                observedPayload = msg->GetPayload(0).ConvertToString();
                observedChecksums.assign(
                    msg->Record.GetChecksums().begin(),
                    msg->Record.GetChecksums().end());
            }
            return NActors::TTestActorRuntime::EEventAction::PROCESS;
        });

    auto future = transport->WriteToDDisk(
        connection,
        NDDisk::TBlockSelector{0, 0, DefaultBlockSize * 2},
        NDDisk::TWriteInstruction(0),
        MakeSgList(writeBuf),
        nullptr);
    fixture.WaitFuture(executor, future, WaitTimeout);

    UNIT_ASSERT(
        future.GetValueSync().GetStatus() ==
        NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
    UNIT_ASSERT_VALUES_EQUAL(writeRequests, 1u);
    UNIT_ASSERT_VALUES_EQUAL(observedPayload, writeBuf);
    UNIT_ASSERT_VALUES_EQUAL(
        observedChecksums.size(),
        expectedChecksums.size());
    for (size_t i = 0; i < expectedChecksums.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(observedChecksums[i], expectedChecksums[i]);
    }
    UNIT_ASSERT_VALUES_EQUAL(
        transport->GetFakeDirectSessionSentEventCount(),
        directSession ? 1u : 0u);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TICDirectStorageTransportTest)
{
    // Without a registered IDirectSession the datapath falls back to the actor
    // path and still completes successfully against the local stub.
    Y_UNIT_TEST_F(FallsBackToActorPathWithoutSession, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        const auto& ddiskId = transport->GetDDiskIds()[0];

        auto connect = transport->Connect(MakeDDiskConnection(ddiskId));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        UNIT_ASSERT(
            connect.ConnectFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        auto connection = MakeDDiskConnection(
            ddiskId,
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString buffer(DefaultBlockSize, 'x');
        auto future = transport->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(buffer),
            nullptr);
        WaitFuture(executor, future, WaitTimeout);
        UNIT_ASSERT(
            future.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
    }

    Y_UNIT_TEST_F(ActorPathDirectWriteCarriesPerBlockChecksums, TDBGFixture)
    {
        CheckDirectWriteChecksums(*this, /*directSession=*/false);
    }

    Y_UNIT_TEST_F(DirectSessionWriteCarriesPerBlockChecksums, TDBGFixture)
    {
        CheckDirectWriteChecksums(*this, /*directSession=*/true);
    }

    // With a fake IDirectSession injected, WriteToDDisk / ReadFromDDisk go
    // through the direct-session Send + cookie-demux path and echo payload.
    Y_UNIT_TEST_F(DirectPathWriteAndRead, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        transport->EnableFakeDirectSession();

        const auto& ddiskId = transport->GetDDiskIds()[0];
        auto connect = transport->Connect(MakeDDiskConnection(ddiskId));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        UNIT_ASSERT(
            connect.ConnectFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        auto connection = MakeDDiskConnection(
            ddiskId,
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString writeBuf(DefaultBlockSize, 'w');
        auto writeFuture = transport->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeBuf),
            nullptr);
        WaitFuture(executor, writeFuture, WaitTimeout);
        UNIT_ASSERT(
            writeFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        TString readBuf(DefaultBlockSize, '\0');
        auto readFuture = transport->ReadFromDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readBuf),
            nullptr);
        WaitFuture(executor, readFuture, WaitTimeout);
        UNIT_ASSERT(
            readFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(writeBuf, readBuf);
    }

    // A shut-down IDirectSession makes Send() return false; Remove() drops the
    // handler and completes the promise with OUTDATED / "Session broken".
    Y_UNIT_TEST_F(DeadSessionCompletesWithOutdated, TDBGFixture)
    {
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());

        auto dead =
            std::make_shared<TFakeDirectSession>(Runtime->GetActorSystem(0));
        dead->Shutdown();
        transport->SetDirectSession(dead);

        const auto& ddiskId = transport->GetDDiskIds()[0];
        auto connection = MakeDDiskConnection(ddiskId, /*guid=*/1);

        TString writeBuf(DefaultBlockSize, 'w');
        auto future = transport->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeBuf),
            nullptr);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT(
            future.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED);
        UNIT_ASSERT_STRINGS_EQUAL(
            future.GetValueSync().GetErrorReason(),
            SessionBrokenErrorMessage);
    }

    Y_UNIT_TEST_F(DirectPathReadCopiesPayload, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        transport->EnableFakeDirectSession();

        const auto& pbufferId = transport->GetPBufferIds()[0];
        auto connect = transport->Connect(MakePBufferConnection(pbufferId));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        auto connection = MakePBufferConnection(
            pbufferId,
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString writeBuf(DefaultBlockSize, 'P');
        auto writeFuture = transport->WriteToPBuffer(
            connection,
            NDDisk::TBlockSelector{1, 0, DefaultBlockSize},
            /*lsn=*/42,
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeBuf),
            nullptr);
        WaitFuture(executor, writeFuture, WaitTimeout);
        UNIT_ASSERT(
            writeFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        TString readBuf(DefaultBlockSize, '\0');
        auto readFuture = transport->ReadFromPBuffer(
            connection,
            NDDisk::TBlockSelector{1, 0, DefaultBlockSize},
            /*lsn=*/42,
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readBuf),
            nullptr);
        WaitFuture(executor, readFuture, WaitTimeout);
        UNIT_ASSERT(
            readFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(writeBuf, readBuf);
    }

    Y_UNIT_TEST_F(DirectPathWriteToManyPBuffersAggregatesReplies, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        transport->EnableFakeDirectSession();

        const auto& pbufferIds = transport->GetPBufferIds();
        UNIT_ASSERT(pbufferIds.size() >= 2);
        transport->SetSplitWriteToManyReplies(
            EConnectionType::PBuffer,
            pbufferIds[0],
            true);

        auto connect = transport->Connect(MakePBufferConnection(pbufferIds[0]));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        auto connection = MakePBufferConnection(
            pbufferIds[0],
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString writeBuf(DefaultBlockSize, 'M');
        std::atomic<ui32> callbackCount{0};
        std::atomic<bool> completed{false};
        THashSet<TString> seenIds;
        auto done = NewPromise<void>();

        const auto protoIds = ToProto(pbufferIds);
        transport->WriteToManyPBuffers(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            /*lsn=*/7,
            NDDisk::TWriteInstruction(0),
            protoIds,
            TDuration::Seconds(1),
            MakeSgList(writeBuf),
            nullptr,
            [&](const auto& result, auto)
            {
                callbackCount.fetch_add(1);
                for (const auto& single: result.GetResult()) {
                    UNIT_ASSERT(
                        single.GetResult().GetStatus() ==
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
                    seenIds.insert(
                        single.GetPersistentBufferId().ShortDebugString());
                }
                if (seenIds.size() >= protoIds.size() &&
                    !completed.exchange(true)) {
                    done.SetValue();
                }
            });

        WaitFuture(executor, done.GetFuture(), WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(callbackCount.load(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(seenIds.size(), protoIds.size());
    }

    Y_UNIT_TEST_F(
        DirectPathWriteToManyPBuffersSessionBrokenCompletesRemaining,
        TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        auto* transportPtr = transport.get();
        transportPtr->EnableFakeDirectSession();

        const auto& pbufferIds = transportPtr->GetPBufferIds();
        UNIT_ASSERT(pbufferIds.size() >= 2);
        transportPtr->SetPendingWriteToPBuffer(
            EConnectionType::PBuffer,
            pbufferIds[0]);

        auto connect =
            transportPtr->Connect(MakePBufferConnection(pbufferIds[0]));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        auto connection = MakePBufferConnection(
            pbufferIds[0],
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString writeBuf(DefaultBlockSize, 'M');
        std::atomic<ui32> callbackCount{0};
        std::atomic<bool> completed{false};
        THashSet<TString> okIds;
        THashSet<TString> outdatedIds;
        auto done = NewPromise<void>();
        const auto protoIds = ToProto(pbufferIds);

        transportPtr->WriteToManyPBuffers(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            /*lsn=*/8,
            NDDisk::TWriteInstruction(0),
            protoIds,
            TDuration::Seconds(1),
            MakeSgList(writeBuf),
            nullptr,
            [&](const auto& result, auto)
            {
                callbackCount.fetch_add(1);
                for (const auto& single: result.GetResult()) {
                    const auto id =
                        single.GetPersistentBufferId().ShortDebugString();
                    if (single.GetResult().GetStatus() ==
                        NKikimrBlobStorage::NDDisk::TReplyStatus::OK)
                    {
                        okIds.insert(id);
                    } else {
                        UNIT_ASSERT(
                            single.GetResult().GetStatus() ==
                            NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED);
                        UNIT_ASSERT_STRINGS_EQUAL(
                            single.GetResult().GetErrorReason(),
                            SessionBrokenErrorMessage);
                        outdatedIds.insert(id);
                    }
                }
                if (okIds.size() + outdatedIds.size() >= protoIds.size() &&
                    !completed.exchange(true))
                {
                    done.SetValue();
                }
            });

        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT_VALUES_EQUAL(callbackCount.load(), 0u);

        transportPtr->ReleasePendingWritePBuffersFirstHalf(
            EConnectionType::PBuffer,
            pbufferIds[0]);
        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(callbackCount.load() >= 1u);
        UNIT_ASSERT(!okIds.empty());
        UNIT_ASSERT(outdatedIds.empty());

        transportPtr->ShutdownFakeDirectSession();
        WaitFuture(executor, done.GetFuture(), WaitTimeout);
        UNIT_ASSERT(!outdatedIds.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            okIds.size() + outdatedIds.size(),
            protoIds.size());
    }

    Y_UNIT_TEST_F(DirectPathUndeliveredCompletesWithError, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        transport->EnableFakeDirectSession();

        // Service id for this DDiskId is never registered.
        const NBsController::TDDiskId missingId(
            transport->GetNodeId(),
            /*pdiskId=*/999,
            /*ddiskSlotId=*/999);
        auto connection = MakeDDiskConnection(missingId, /*guid=*/1);

        TString writeBuf(DefaultBlockSize, 'u');
        auto future = transport->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeBuf),
            nullptr);
        WaitFuture(executor, future, WaitTimeout);
        UNIT_ASSERT(
            future.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
        UNIT_ASSERT_STRINGS_EQUAL(
            future.GetValueSync().GetErrorReason(),
            UndeliveryErrorMessage);
    }

    // Actor-path fallback: a held PBuffer read must be rejected on node
    // disconnect via RejectAllSessionRequestsForNode (not only DDisk maps).
    Y_UNIT_TEST_F(ActorPathPBufferReadRejectedOnDisconnect, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        auto* transportPtr = transport.get();

        const auto& pbufferId = transportPtr->GetPBufferIds()[0];
        auto connect = transportPtr->Connect(MakePBufferConnection(pbufferId));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        auto connection = MakePBufferConnection(
            pbufferId,
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        transportPtr->SetPendingReadFromDDisk(
            EConnectionType::PBuffer,
            pbufferId);

        TString readBuf(DefaultBlockSize, '\0');
        auto future = transportPtr->ReadFromPBuffer(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            /*lsn=*/1,
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readBuf),
            nullptr);
        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(!future.HasValue());

        transportPtr->FireDisconnect(
            EConnectionType::PBuffer,
            pbufferId,
            transportPtr->GetNodeId());
        WaitFuture(executor, future, WaitTimeout);
        UNIT_ASSERT(
            future.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OUTDATED);
        UNIT_ASSERT_STRINGS_EQUAL(
            future.GetValueSync().GetErrorReason(),
            SessionBrokenErrorMessage);
    }

    Y_UNIT_TEST_F(DirectPathEraseSyncList, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        transport->EnableFakeDirectSession();

        const auto& pbufferId = transport->GetPBufferIds()[0];
        const auto& ddiskId = transport->GetDDiskIds()[0];

        auto pbConnect = transport->Connect(MakePBufferConnection(pbufferId));
        WaitFuture(executor, pbConnect.ConnectFuture, WaitTimeout);
        auto pbConnection = MakePBufferConnection(
            pbufferId,
            pbConnect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        auto ddConnect = transport->Connect(MakeDDiskConnection(ddiskId));
        WaitFuture(executor, ddConnect.ConnectFuture, WaitTimeout);
        auto ddConnection = MakeDDiskConnection(
            ddiskId,
            ddConnect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        auto batch = transport->BatchEraseFromPBuffer(
            pbConnection,
            TVector<ui64>{1, 2, 3},
            nullptr);
        WaitFuture(executor, batch, WaitTimeout);
        UNIT_ASSERT(
            batch.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        auto barrier = transport->BarrierEraseFromPBuffer(
            pbConnection,
            /*lsn=*/10,
            nullptr);
        WaitFuture(executor, barrier, WaitTimeout);
        UNIT_ASSERT(
            barrier.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        auto sync = transport->SyncWithPBuffer(
            pbConnection,
            ddConnection,
            TVector{NDDisk::TBlockSelector{0, 0, DefaultBlockSize}},
            TVector<ui64>{1},
            nullptr);
        WaitFuture(executor, sync, WaitTimeout);
        UNIT_ASSERT(
            sync.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        auto list = transport->ListPBufferEntries(pbConnection);
        WaitFuture(executor, list, WaitTimeout);
        UNIT_ASSERT(
            list.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);
    }

    Y_UNIT_TEST_F(DirectPathConcurrentRequestsDoNotCrossTalk, TDBGFixture)
    {
        auto executor = MakeExecutor();
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        auto* transportPtr = transport.get();
        transportPtr->EnableFakeDirectSession();

        const auto& ddiskId = transportPtr->GetDDiskIds()[0];
        auto connect = transportPtr->Connect(MakeDDiskConnection(ddiskId));
        WaitFuture(executor, connect.ConnectFuture, WaitTimeout);
        auto connection = MakeDDiskConnection(
            ddiskId,
            connect.ConnectFuture.GetValueSync().GetDDiskInstanceGuid());

        TString writeA(DefaultBlockSize, 'A');
        auto writeAFuture = transportPtr->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeA),
            nullptr);
        WaitFuture(executor, writeAFuture, WaitTimeout);

        TString writeB(DefaultBlockSize, 'B');
        transportPtr->SetPendingWriteToDDisk(EConnectionType::DDisk, ddiskId);
        transportPtr->SetPendingReadFromDDisk(EConnectionType::DDisk, ddiskId);

        auto writeBFuture = transportPtr->WriteToDDisk(
            connection,
            NDDisk::TBlockSelector{0, DefaultBlockSize, DefaultBlockSize},
            NDDisk::TWriteInstruction(0),
            MakeSgList(writeB),
            nullptr);

        TString readA(DefaultBlockSize, '\0');
        auto readAFuture = transportPtr->ReadFromDDisk(
            connection,
            NDDisk::TBlockSelector{0, 0, DefaultBlockSize},
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readA),
            nullptr);

        TString readB(DefaultBlockSize, '\0');
        auto readBFuture = transportPtr->ReadFromDDisk(
            connection,
            NDDisk::TBlockSelector{0, DefaultBlockSize, DefaultBlockSize},
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readB),
            nullptr);

        DoAllExecutorAndRuntimeWork(executor);
        UNIT_ASSERT(!writeBFuture.HasValue());
        UNIT_ASSERT(!readAFuture.HasValue());
        UNIT_ASSERT(!readBFuture.HasValue());

        // Release out of order: reads first (still see only A for offset 0;
        // offset DefaultBlockSize empty until write B is released), then write.
        transportPtr->ReleasePendingReads(EConnectionType::DDisk, ddiskId);
        WaitFuture(executor, readAFuture, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(writeA, readA);

        transportPtr->ReleasePendingWrites(EConnectionType::DDisk, ddiskId);
        WaitFuture(executor, writeBFuture, WaitTimeout);
        UNIT_ASSERT(
            writeBFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        // readB was released before writeB stored its payload — empty or old.
        WaitFuture(executor, readBFuture, WaitTimeout);
        UNIT_ASSERT(
            readBFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::OK);

        // Fresh read after writeB must return B — proves cookie demux did not
        // complete the wrong future earlier and the write landed correctly.
        TString readB2(DefaultBlockSize, '\0');
        auto readB2Future = transportPtr->ReadFromDDisk(
            connection,
            NDDisk::TBlockSelector{0, DefaultBlockSize, DefaultBlockSize},
            NDDisk::TReadInstruction(/*returnInRopePayload=*/true),
            MakeSgList(readB2),
            nullptr);
        WaitFuture(executor, readB2Future, WaitTimeout);
        UNIT_ASSERT_VALUES_EQUAL(writeB, readB2);
    }

    Y_UNIT_TEST_F(DestroysOwnedActorAndRejectsPendingConnect, TDBGFixture)
    {
        auto transport =
            std::make_unique<TICStorageTransportTestAdapter>(Runtime.get());
        const TActorId transportActorId = transport->GetTransportActorId();
        const auto& ddiskId = transport->GetDDiskIds()[0];

        transport->SetPendingConnect(EConnectionType::DDisk, ddiskId);
        auto connect = transport->Connect(MakeDDiskConnection(ddiskId));
        DrainRuntime();

        UNIT_ASSERT(Runtime->FindActor(transportActorId));
        UNIT_ASSERT(!connect.ConnectFuture.HasValue());

        transport.reset();
        DrainRuntime();

        UNIT_ASSERT(!Runtime->FindActor(transportActorId));
        UNIT_ASSERT(connect.ConnectFuture.HasValue());
        UNIT_ASSERT(
            connect.ConnectFuture.GetValueSync().GetStatus() ==
            NKikimrBlobStorage::NDDisk::TReplyStatus::ERROR);
        UNIT_ASSERT_STRINGS_EQUAL(
            DestroyErrorMessage,
            connect.ConnectFuture.GetValueSync().GetErrorReason());
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
