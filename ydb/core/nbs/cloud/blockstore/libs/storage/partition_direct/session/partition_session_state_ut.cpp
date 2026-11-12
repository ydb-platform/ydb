#include "partition_session_state.h"

#include "events.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

NKikimrBlockStore::TVolumeConfig MakeMetadata(ui32 blockSize = DefaultBlockSize)
{
    NKikimrBlockStore::TVolumeConfig config;
    config.SetDiskId("disk1");
    config.SetBlockSize(blockSize);
    config.AddPartitions()->SetBlockCount(33554432);
    config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
    return config;
}

TVolumeConfigPtr MakeGeometry(const NKikimrBlockStore::TVolumeConfig& config)
{
    return std::make_shared<TVolumeConfig>(TVolumeConfig{
        .DiskId = config.GetDiskId(),
        .BlockSize = config.GetBlockSize(),
        .BlockCount = config.GetPartitions(0).GetBlockCount(),
        .BlocksPerStripe = 512_KB / config.GetBlockSize(),
        .VChunkSize = 128_MB,
    });
}

TIntrusivePtr<TPartitionSessionState> MakeState()
{
    const auto config = MakeMetadata();
    auto result = TPartitionSessionState::Create(
        config,
        std::make_shared<TTestStorage>(),
        MakeGeometry(config));
    UNIT_ASSERT(!HasError(result));
    return result.ExtractResult();
}

}   // namespace

Y_UNIT_TEST_SUITE(TPartitionSessionStateTest)
{
    Y_UNIT_TEST(ShouldRejectUnsupportedMediaKind)
    {
        auto config = MakeMetadata();
        config.SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        UNIT_ASSERT_VALUES_EQUAL(
            TPartitionSession::Create(
                config,
                std::make_shared<TTestStorage>(),
                MakeGeometry(config))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldPublishSessionVersionsAndReuseBackend)
    {
        auto config = MakeMetadata();
        const auto state = TPartitionSessionState::Create(
                               config,
                               std::make_shared<TTestStorage>(),
                               MakeGeometry(config))
                               .ExtractResult();
        config.SetDiskId("changed");
        UNIT_ASSERT_VALUES_EQUAL(
            state->GetVolumeMetadata().GetDiskId(),
            "disk1");
        THotSwap<TPartitionSessionState> holder(state);
        auto mounted =
            MakeIntrusive<TPartitionSessionState>(*holder.AtomicLoad());
        const auto first = mounted->Mount("client").ExtractResult();
        holder.AtomicStore(mounted);
        // Publishing a session must not modify the version retained by readers.
        UNIT_ASSERT_VALUES_EQUAL(
            state->AcquireIoBackend("client", first).GetError().GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            mounted->GetRegistrationId(),
            state->GetRegistrationId());
        UNIT_ASSERT(
            &mounted->GetVolumeMetadata() == &state->GetVolumeMetadata());
        const auto backend = holder.AtomicLoad()
                                 ->AcquireIoBackend("client", first)
                                 .ExtractResult();
        auto remounted =
            MakeIntrusive<TPartitionSessionState>(*holder.AtomicLoad());
        UNIT_ASSERT_VALUES_EQUAL(remounted->Mount("client").GetResult(), first);
        holder.AtomicStore(remounted);
        const auto remountedBackend = holder.AtomicLoad()
                                          ->AcquireIoBackend("client", first)
                                          .ExtractResult();
        UNIT_ASSERT(backend.Handler == remountedBackend.Handler);
        UNIT_ASSERT(backend.IoGeometry == remountedBackend.IoGeometry);

        auto unmounted =
            MakeIntrusive<TPartitionSessionState>(*holder.AtomicLoad());
        UNIT_ASSERT(!HasError(unmounted->Unmount("client", first)));
        holder.AtomicStore(unmounted);
        UNIT_ASSERT_VALUES_EQUAL(
            holder.AtomicLoad()
                ->AcquireIoBackend("client", first)
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(
            mounted->AcquireIoBackend("client", first).GetResult().Handler ==
            backend.Handler);
        UNIT_ASSERT_VALUES_EQUAL(
            holder.AtomicLoad()->GetRegistrationId(),
            state->GetRegistrationId());
    }

    Y_UNIT_TEST(ShouldCreateBackendForNativeBlockSizes)
    {
        for (ui32 blockSize = DefaultBlockSize; blockSize <= MaxBlockSize;
             blockSize *= 2)
        {
            const auto config = MakeMetadata(blockSize);
            const auto state = TPartitionSessionState::Create(
                                   config,
                                   std::make_shared<TTestStorage>(),
                                   MakeGeometry(config))
                                   .ExtractResult();
            const auto session = state->Mount("client").ExtractResult();
            const auto backend =
                state->AcquireIoBackend("client", session).ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(backend.IoGeometry->BlockSize, blockSize);
        }
    }

    Y_UNIT_TEST(ShouldPublishChangesThroughPartitionSession)
    {
        const TString clientId = "client";
        const TString otherClientId = "other";
        const TString invalidSessionId = "wrong";

        const auto config = MakeMetadata();
        const auto session = TPartitionSession::Create(
                                 config,
                                 std::make_shared<TTestStorage>(),
                                 MakeGeometry(config))
                                 .ExtractResult();
        const auto registrationId = session->GetRegistrationId();
        const auto* metadata = &session->GetVolumeMetadata();
        const auto mounted = session->Mount(clientId).ExtractResult();
        const auto backend =
            session->AcquireIoBackend(clientId, mounted).ExtractResult();

        UNIT_ASSERT_VALUES_EQUAL(
            session->Mount(otherClientId).GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(
            session->Unmount(clientId, invalidSessionId).GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(
            session->AcquireIoBackend(clientId, mounted).GetResult().Handler ==
            backend.Handler);

        UNIT_ASSERT(!HasError(session->Unmount(clientId, mounted)));
        UNIT_ASSERT_VALUES_EQUAL(
            session->AcquireIoBackend(clientId, mounted).GetError().GetCode(),
            E_BS_INVALID_SESSION);
        const auto remounted = session->Mount(otherClientId).ExtractResult();
        UNIT_ASSERT(
            !HasError(session->AcquireIoBackend(otherClientId, remounted)));
        session->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            session->Mount(clientId).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            session->Unmount(otherClientId, remounted).GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            session->AcquireIoBackend(otherClientId, remounted)
                .GetError()
                .GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(session->GetRegistrationId(), registrationId);
        UNIT_ASSERT(&session->GetVolumeMetadata() == metadata);
    }

    Y_UNIT_TEST(ShouldValidateIdentityAndRevokeOnlyMatchingSession)
    {
        auto state = MakeState();
        UNIT_ASSERT_VALUES_EQUAL(
            state->Mount("").GetError().GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            state->Unmount("client", "unknown").GetCode(),
            S_ALREADY);
        const auto first = state->Mount("client").ExtractResult();
        UNIT_ASSERT(!first.empty());
        UNIT_ASSERT_VALUES_EQUAL(
            state->Mount("other").GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        const auto checkInvalidIdentity =
            [&](const TString& client, const TString& session)
        {
            UNIT_ASSERT_VALUES_EQUAL(
                state->Unmount(client, session).GetCode(),
                E_BS_INVALID_SESSION);
            UNIT_ASSERT_VALUES_EQUAL(
                state->AcquireIoBackend(client, session).GetError().GetCode(),
                E_BS_INVALID_SESSION);
            UNIT_ASSERT(!HasError(state->AcquireIoBackend("client", first)));
        };
        checkInvalidIdentity("", first);
        checkInvalidIdentity("other", first);
        checkInvalidIdentity("client", "");
        checkInvalidIdentity("client", "stale");
        UNIT_ASSERT_VALUES_EQUAL(
            state->Unmount("client", first).GetCode(),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            state->Unmount("client", first).GetCode(),
            S_ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(
            state->AcquireIoBackend("client", first).GetError().GetCode(),
            E_BS_INVALID_SESSION);
        const auto second = state->Mount("other").ExtractResult();
        UNIT_ASSERT(second != first);
        UNIT_ASSERT_VALUES_EQUAL(
            state->AcquireIoBackend("other", first).GetError().GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            state->Unmount("client", first).GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(!HasError(state->AcquireIoBackend("other", second)));
        auto replacement = MakeState();
        UNIT_ASSERT(
            replacement->GetRegistrationId() != state->GetRegistrationId());
        UNIT_ASSERT_VALUES_EQUAL(
            replacement->AcquireIoBackend("other", second).GetError().GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(replacement->Mount("other").GetResult() != second);
    }

    Y_UNIT_TEST(ShouldDetachRetainedBackendOnPartitionStop)
    {
        auto storage = std::make_shared<TTestStorage>();
        storage->ReadBlocksLocalHandler =
            [](TCallContextPtr context, auto request)
        {
            Y_UNUSED(context);
            Y_UNUSED(request);
            return NThreading::MakeFuture<TReadBlocksLocalResponse>();
        };
        const auto config = MakeMetadata();
        auto state = TPartitionSessionState::Create(
                         config,
                         storage,
                         MakeGeometry(config))
                         .ExtractResult();
        const auto session = state->Mount("client").ExtractResult();
        auto backend =
            state->AcquireIoBackend("client", session).ExtractResult();
        TGuardedBuffer buffer(TString(DefaultBlockSize, '\0'));
        auto read = [&]
        {
            return backend.Handler
                ->Read(
                    MakeIntrusive<TCallContext>(),
                    0,
                    DefaultBlockSize,
                    buffer.GetGuardedSgList(),
                    {})
                .GetValueSync();
        };
        UNIT_ASSERT(!HasError(read().Error));
        auto stopped = MakeIntrusive<TPartitionSessionState>(*state);
        stopped->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            stopped->Mount("client").GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            stopped->Unmount("client", session).GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            stopped->AcquireIoBackend("client", session).GetError().GetCode(),
            E_REJECTED);
        // Old versions keep their session data but share the detached backend.
        UNIT_ASSERT(!HasError(state->AcquireIoBackend("client", session)));
        UNIT_ASSERT_VALUES_EQUAL(read().Error.GetCode(), E_REJECTED);
    }

    Y_UNIT_TEST(ShouldCompleteDroppedControlRequests)
    {
        auto mount = std::make_unique<TEvPartitionSession::TEvMount>("client");
        auto mounted = mount->Result.GetFuture();
        mount.reset();
        UNIT_ASSERT(mounted.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            mounted.GetValue().GetError().GetCode(),
            E_REJECTED);
        auto unmount = std::make_unique<TEvPartitionSession::TEvUnmount>(
            "client",
            "session");
        auto unmounted = unmount->Result.GetFuture();
        unmount.reset();
        UNIT_ASSERT(unmounted.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(unmounted.GetValue().GetCode(), E_REJECTED);
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
