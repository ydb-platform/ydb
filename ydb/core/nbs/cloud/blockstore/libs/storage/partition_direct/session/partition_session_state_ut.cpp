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
        .BlockCount = config.PartitionsSize()
                          ? config.GetPartitions(0).GetBlockCount()
                          : 0,
        .BlocksPerStripe =
            config.GetBlockSize() ? 512_KB / config.GetBlockSize() : 0,
        .VChunkSize = 128_MB,
    });
}

std::shared_ptr<TPartitionSessionState> MakeState()
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
    Y_UNIT_TEST(ShouldValidateMetadataAndBackend)
    {
        const auto good = MakeMetadata();
        auto storage = std::make_shared<TTestStorage>();
        TVector<NKikimrBlockStore::TVolumeConfig> invalid;
        invalid.push_back(good);
        invalid.back().ClearDiskId();
        invalid.push_back(good);
        invalid.back().ClearPartitions();
        invalid.push_back(good);
        invalid.back().AddPartitions()->SetBlockCount(1);
        invalid.push_back(good);
        invalid.back().SetBlockSize(512);
        invalid.push_back(good);
        invalid.back().MutablePartitions(0)->SetBlockCount(0);
        invalid.push_back(good);
        invalid.back().SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        for (const auto& config: invalid) {
            UNIT_ASSERT_VALUES_EQUAL(
                TPartitionSessionState::Create(
                    config,
                    storage,
                    MakeGeometry(config))
                    .GetError()
                    .GetCode(),
                E_ARGUMENT);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TPartitionSessionState::Create(good, {}, MakeGeometry(good))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            TPartitionSessionState::Create(good, storage, {})
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        for (ui32 i = 0; i != 5; ++i) {
            const auto geometry = MakeGeometry(good);
            auto other = std::make_shared<TVolumeConfig>(TVolumeConfig{
                .DiskId = i == 0 ? TString("other") : geometry->DiskId,
                .BlockSize =
                    i == 1 ? geometry->BlockSize * 2 : geometry->BlockSize,
                .BlockCount =
                    i == 2 ? geometry->BlockCount - 1 : geometry->BlockCount,
                .BlocksPerStripe = i == 3 ? 0 : geometry->BlocksPerStripe,
                .VChunkSize = i == 4 ? 0 : geometry->VChunkSize,
            });
            UNIT_ASSERT_VALUES_EQUAL(
                TPartitionSessionState::Create(good, storage, other)
                    .GetError()
                    .GetCode(),
                E_ARGUMENT);
        }
    }

    Y_UNIT_TEST(ShouldOwnMetadataAndReuseHandler)
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
        const auto first = state->Mount("client").ExtractResult();
        const auto backend =
            state->AcquireIoBackend("client", first).ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(state->Mount("client").GetResult(), first);
        UNIT_ASSERT(
            backend.Handler ==
            state->AcquireIoBackend("client", first).GetResult().Handler);
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
        state->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            state->Mount("client").GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state->Unmount("client", session).GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state->AcquireIoBackend("client", session).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(read().Error.GetCode(), E_REJECTED);
    }

    Y_UNIT_TEST(ShouldCompleteDroppedControlRequests)
    {
        auto mount = std::make_unique<TEvPartitionSession::TEvMount>(
            "registration",
            "client");
        auto mounted = mount->Result.GetFuture();
        mount.reset();
        UNIT_ASSERT(mounted.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            mounted.GetValue().GetError().GetCode(),
            E_REJECTED);
        auto unmount = std::make_unique<TEvPartitionSession::TEvUnmount>(
            "registration",
            "client",
            "session");
        auto unmounted = unmount->Result.GetFuture();
        unmount.reset();
        UNIT_ASSERT(unmounted.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(unmounted.GetValue().GetCode(), E_REJECTED);
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
