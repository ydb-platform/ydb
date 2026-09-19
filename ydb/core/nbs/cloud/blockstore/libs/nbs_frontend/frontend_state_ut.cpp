#include "frontend_state.h"

#include "frontend_test.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <atomic>
#include <barrier>
#include <thread>

namespace NYdb::NBS::NBlockStore {

namespace {

using namespace NTests;

namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

}   // namespace

Y_UNIT_TEST_SUITE(TFrontendStateMetadataTest)
{
    Y_UNIT_TEST(ShouldAcceptNativeBlockSizes)
    {
        for (ui32 blockSize = DefaultBlockSize; blockSize <= MaxBlockSize;
             blockSize *= 2)
        {
            TFrontendState state;
            const auto config = MakeTestVolumeConfig(blockSize);
            UNIT_ASSERT(!HasError(RegisterTestVolume(state, config)));
            state.Start();
            const auto mounted = state.MountVolume(MakeTestMountRequest());
            UNIT_ASSERT(!HasError(mounted));
            UNIT_ASSERT_VALUES_EQUAL(
                mounted.GetVolume().GetBlockSize(),
                blockSize);
            const auto backend = state.AcquireIoBackend(
                TestDiskId,
                TestClientId,
                mounted.GetSessionId());
            UNIT_ASSERT(!HasError(backend));
            UNIT_ASSERT(backend.GetResult().Handler);
            UNIT_ASSERT_VALUES_EQUAL(
                backend.GetResult().IoGeometry->BlockSize,
                blockSize);
        }
    }

    Y_UNIT_TEST(ShouldRejectMissingOrInconsistentBackend)
    {
        TFrontendState state;
        const auto config = MakeTestVolumeConfig();
        auto storage = std::make_shared<TTestStorage>();
        UNIT_ASSERT_VALUES_EQUAL(
            state.RegisterVolume(config, {}, MakeTestIoConfig(config))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            state.RegisterVolume(config, storage, {}).GetError().GetCode(),
            E_ARGUMENT);
        auto other = config;
        other.SetBlockSize(config.GetBlockSize() * 2);
        UNIT_ASSERT_VALUES_EQUAL(
            state.RegisterVolume(config, storage, MakeTestIoConfig(other))
                .GetError()
                .GetCode(),
            E_ARGUMENT);
    }

    Y_UNIT_TEST(ShouldCopyMetadataAndConvertClassicVolume)
    {
        TFrontendState state;
        auto config = MakeTestVolumeConfig();
        const auto registration = RegisterTestVolume(state, config);
        UNIT_ASSERT(!HasError(registration));
        UNIT_ASSERT(!registration.GetResult().empty());

        const auto expected = config;

        // Publication owns a copy; actor-side mutation cannot change it.
        config.SetDiskId("changed");
        config.MutablePartitions(0)->SetBlockCount(1);
        state.Start();
        const auto result = state.GetVolume(TestDiskId);
        UNIT_ASSERT(!HasError(result));
        const auto& volume = result.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(volume.GetDiskId(), expected.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            volume.GetBlockSize(),
            expected.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(
            volume.GetBlocksCount(),
            expected.GetPartitions(0).GetBlockCount());
        UNIT_ASSERT_VALUES_EQUAL(volume.GetPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(volume.GetStorageMediaKind()),
            static_cast<ui32>(NNbs1CompatApi::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(
            volume.GetConfigVersion(),
            expected.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(
            volume.GetProjectId(),
            expected.GetProjectId());
        UNIT_ASSERT_VALUES_EQUAL(volume.GetFolderId(), expected.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(volume.GetCloudId(), expected.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL(volume.DevicesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(volume.MigrationsSize(), 0);
    }

    Y_UNIT_TEST(ShouldRejectInvalidMetadataWithoutReplacingRegistration)
    {
        TFrontendState state;
        const auto good = MakeTestVolumeConfig();
        UNIT_ASSERT(!HasError(RegisterTestVolume(state, good)));
        state.Start();

        TVector<NKikimrBlockStore::TVolumeConfig> invalid(7, good);
        invalid[0].ClearDiskId();
        invalid[1].ClearPartitions();
        invalid[2].AddPartitions()->SetBlockCount(1);
        constexpr ui32 unsupportedBlockSize = 512;
        invalid[3].SetBlockSize(unsupportedBlockSize);
        invalid[4].MutablePartitions(0)->SetBlockCount(0);
        invalid[5].SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        invalid[6].SetDiskId("another-disk");
        for (const auto& config: invalid) {
            UNIT_ASSERT_VALUES_EQUAL(
                RegisterTestVolume(state, config).GetError().GetCode(),
                E_ARGUMENT);
            const auto volume = state.GetVolume(TestDiskId);
            UNIT_ASSERT(!HasError(volume));
            UNIT_ASSERT_VALUES_EQUAL(
                volume.GetResult().GetBlocksCount(),
                good.GetPartitions(0).GetBlockCount());
        }
    }

    Y_UNIT_TEST(ShouldGuardReplacementFromOldUnregister)
    {
        TFrontendState state;
        auto config = MakeTestVolumeConfig();
        const auto first = RegisterTestVolume(state, config);
        UNIT_ASSERT(!HasError(first));
        // The fixture size is not a frontend constant.
        config.MutablePartitions(0)->SetBlockCount(1024);
        const auto second = RegisterTestVolume(state, config);
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(first.GetResult() != second.GetResult());
        state.Start();
        state.UnregisterVolume(first.GetResult());
        const auto volume = state.GetVolume(TestDiskId);
        UNIT_ASSERT(!HasError(volume));
        UNIT_ASSERT_VALUES_EQUAL(volume.GetResult().GetBlocksCount(), 1024);
        state.UnregisterVolume(second.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_NOT_FOUND);
        state.UnregisterVolume(second.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldPreserveRegistrationAcrossStopStart)
    {
        TFrontendState state;
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT(!HasError(state.CheckAcceptingRequests()));
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_NOT_FOUND);

        const auto registration =
            RegisterTestVolume(state, MakeTestVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        state.Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            state.CheckAcceptingRequests().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT(!HasError(state.GetVolume(TestDiskId)));
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("").GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("other").GetError().GetCode(),
            E_NOT_FOUND);

        // A stopped frontend must still process partition teardown.
        state.Stop();
        state.UnregisterVolume(registration.GetResult());
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldNotReuseRegistrationFromPreviousRuntime)
    {
        TString oldRegistration;
        {
            TFrontendState oldState;
            const auto registration =
                RegisterTestVolume(oldState, MakeTestVolumeConfig());
            UNIT_ASSERT(!HasError(registration));
            oldRegistration = registration.GetResult();
        }
        TFrontendState state;
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume(TestDiskId).GetError().GetCode(),
            E_NOT_FOUND);
        const auto registration =
            RegisterTestVolume(state, MakeTestVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        UNIT_ASSERT(registration.GetResult() != oldRegistration);
        state.UnregisterVolume(oldRegistration);
        UNIT_ASSERT(!HasError(state.GetVolume(TestDiskId)));
    }
}

Y_UNIT_TEST_SUITE(TFrontendStateSessionTest)
{
    Y_UNIT_TEST(ShouldReuseHandlerOnRemount)
    {
        TFrontendState state;
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        state.Start();
        const auto request = MakeTestMountRequest();
        const auto first = state.MountVolume(request);
        UNIT_ASSERT(!HasError(first));
        const auto backend = state.AcquireIoBackend(
            request.GetDiskId(),
            request.GetHeaders().GetClientId(),
            first.GetSessionId());
        UNIT_ASSERT(!HasError(backend));

        const auto remounted = state.MountVolume(request);
        UNIT_ASSERT(!HasError(remounted));
        UNIT_ASSERT_VALUES_EQUAL(
            remounted.GetSessionId(),
            first.GetSessionId());
        const auto repeated = state.AcquireIoBackend(
            request.GetDiskId(),
            request.GetHeaders().GetClientId(),
            remounted.GetSessionId());
        UNIT_ASSERT(!HasError(repeated));
        UNIT_ASSERT(
            backend.GetResult().Handler == repeated.GetResult().Handler);
    }

    Y_UNIT_TEST(ShouldDetachOnlyMatchingBackendRegistration)
    {
        TFrontendState state;
        state.Start();
        const auto config = MakeTestVolumeConfig();
        auto storage = std::make_shared<TTestStorage>();
        storage->ReadBlocksLocalHandler =
            [](TCallContextPtr context, auto request)
        {
            Y_UNUSED(context);
            Y_UNUSED(request);
            return NThreading::MakeFuture<TReadBlocksLocalResponse>();
        };
        const auto firstRegistration =
            state.RegisterVolume(config, storage, MakeTestIoConfig(config));
        UNIT_ASSERT(!HasError(firstRegistration));
        const auto firstSession =
            state.MountVolume(MakeTestMountRequest()).GetSessionId();
        const auto first =
            state.AcquireIoBackend(TestDiskId, TestClientId, firstSession)
                .ExtractResult();
        auto changed = config;
        changed.MutablePartitions(0)->SetBlockCount(1024);
        const auto secondConfig = MakeTestIoConfig(changed);
        const auto secondRegistration =
            state.RegisterVolume(changed, storage, secondConfig);
        UNIT_ASSERT(!HasError(secondRegistration));
        const auto secondSession =
            state.MountVolume(MakeTestMountRequest()).GetSessionId();
        const auto second =
            state.AcquireIoBackend(TestDiskId, TestClientId, secondSession)
                .ExtractResult();
        UNIT_ASSERT(first.Handler != second.Handler);
        UNIT_ASSERT(second.IoGeometry == secondConfig);
        UNIT_ASSERT_VALUES_EQUAL(
            first.IoGeometry->BlockCount,
            config.GetPartitions(0).GetBlockCount());

        TGuardedBuffer buffer(TString(config.GetBlockSize(), '\0'));
        const auto read = [&](const TFrontendIoBackend& backend)
        {
            return backend.Handler
                ->Read(
                    MakeIntrusive<TCallContext>(),
                    0,
                    config.GetBlockSize(),
                    buffer.GetGuardedSgList(),
                    {})
                .GetValueSync()
                .Error.GetCode();
        };
        UNIT_ASSERT_VALUES_EQUAL(read(first), E_REJECTED);
        state.UnregisterVolume(firstRegistration.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(read(second), S_OK);
        state.UnregisterVolume(secondRegistration.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(read(second), E_REJECTED);
    }

    Y_UNIT_TEST(ShouldMountIdempotentlyAndIgnoreNonIdentityParameters)
    {
        TFrontendState state;
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        state.Start();
        auto request = MakeTestMountRequest();
        const auto first = state.MountVolume(request);
        UNIT_ASSERT(!HasError(first));
        UNIT_ASSERT(!first.GetSessionId().empty());
        UNIT_ASSERT_VALUES_EQUAL(first.GetInactiveClientsTimeout(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            first.GetVolume().SerializeAsString(),
            state.GetVolume(TestDiskId).GetResult().SerializeAsString());

        request.SetInstanceId("another-vm");
        request.SetIpcType(NCompatProto::IPC_VHOST);
        request.SetClientVersionInfo("another-version");
        request.SetForceRemoteBinding(true);
        request.SetVolumeMountMode(NCompatProto::VOLUME_MOUNT_REMOTE);
        request.MutableEncryptionSpec()->SetMode(NCompatProto::NO_ENCRYPTION);
        request.MutableHeaders()->SetRequestId(42);
        request.MutableHeaders()->SetTraceId("another-trace");
        const auto repeated = state.MountVolume(request);
        UNIT_ASSERT(!HasError(repeated));
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetSessionId(), first.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(repeated.GetInactiveClientsTimeout(), 0);

        request.MutableHeaders()->SetClientId("client2");
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        UNIT_ASSERT(!HasError(state.AcquireIoBackend(
            TestDiskId,
            TestClientId,
            first.GetSessionId())));
        UNIT_ASSERT(!HasError(state.UnmountVolume(
            TestDiskId,
            TestClientId,
            first.GetSessionId())));
        const auto next = state.MountVolume(request);
        UNIT_ASSERT(!HasError(next));
        UNIT_ASSERT(next.GetSessionId() != first.GetSessionId());
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMountParametersWithoutChangingSession)
    {
        TVector<NCompatProto::TMountVolumeRequest> invalid(
            15,
            MakeTestMountRequest());
        invalid[0].SetVolumeAccessMode(NCompatProto::VOLUME_ACCESS_READ_ONLY);
        invalid[1].SetVolumeAccessMode(NCompatProto::VOLUME_ACCESS_REPAIR);
        invalid[2].SetVolumeAccessMode(
            NCompatProto::VOLUME_ACCESS_USER_READ_ONLY);
        invalid[3].SetVolumeMountMode(
            static_cast<NCompatProto::EVolumeMountMode>(42));
        invalid[4].SetMountSeqNumber(1);
        invalid[5].SetFillSeqNumber(1);
        invalid[6].SetFillGeneration(1);
        invalid[7].SetMountFlags(1);
        invalid[8].SetThrottlingDisabled(true);
        invalid[9].SetToken("token");
        invalid[10].SetForceDisableEncryption(true);
        invalid[11].MutableEncryptionSpec()->SetMode(
            NCompatProto::ENCRYPTION_AES_XTS);
        invalid[12].MutableEncryptionSpec()->SetMode(
            NCompatProto::ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY);
        invalid[13].MutableEncryptionSpec()->MutableKeyPath();
        // Even an explicitly supplied empty key parameter is unsupported.
        invalid[14].MutableEncryptionSpec()->SetKeyHash("");
        for (const auto& request: invalid) {
            TFrontendState state;
            UNIT_ASSERT(
                !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
            state.Start();
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                state.UnmountVolume(TestDiskId, TestClientId, "unknown")
                    .GetCode(),
                S_ALREADY);
            const auto first = state.MountVolume(MakeTestMountRequest());
            UNIT_ASSERT(!HasError(first));
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(MakeTestMountRequest()).GetSessionId(),
                first.GetSessionId());
        }
    }

    Y_UNIT_TEST(ShouldApplyMountErrorPriority)
    {
        TFrontendState state;
        auto request = MakeTestMountRequest();
        request.MutableHeaders()->ClearClientId();
        request.SetMountSeqNumber(1);
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_ARGUMENT);
        const auto first = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(first));
        request.MutableHeaders()->SetClientId("another-client");
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_NOT_IMPLEMENTED);
        request.SetMountSeqNumber(0);
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        request.SetDiskId("");
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(state.AcquireIoBackend(
            TestDiskId,
            TestClientId,
            first.GetSessionId())));
    }

    Y_UNIT_TEST(ShouldValidateUnmountAndIoWithoutRevokingAnotherSession)
    {
        TFrontendState state;
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("", "", "").GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state.AcquireIoBackend("", "", "").GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, "", "").GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            state.AcquireIoBackend(TestDiskId, "", "").GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, "", "unknown").GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, TestClientId, "").GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, TestClientId, "unknown").GetCode(),
            S_ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(
            state.AcquireIoBackend(TestDiskId, TestClientId, "unknown")
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);

        const auto first = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(first));
        const auto& id = first.GetSessionId();

        struct TInvalidRequest
        {
            TString DiskId;
            TString ClientId;
            TString SessionId;
            ui32 Error;
        };

        const TVector<TInvalidRequest> invalid = {
            {"", TestClientId, id, E_NOT_FOUND},
            {"other-disk", "", "", E_NOT_FOUND},
            {TestDiskId, "", id, E_BS_INVALID_SESSION},
            {TestDiskId, TestClientId, "", E_BS_INVALID_SESSION},
            {TestDiskId, "client2", id, E_BS_INVALID_SESSION},
            {TestDiskId, TestClientId, "old-token", E_BS_INVALID_SESSION},
        };
        for (const auto& request: invalid) {
            UNIT_ASSERT_VALUES_EQUAL(
                state
                    .UnmountVolume(
                        request.DiskId,
                        request.ClientId,
                        request.SessionId)
                    .GetCode(),
                request.Error);
            UNIT_ASSERT_VALUES_EQUAL(
                state
                    .AcquireIoBackend(
                        request.DiskId,
                        request.ClientId,
                        request.SessionId)
                    .GetError()
                    .GetCode(),
                request.Error);
            UNIT_ASSERT(!HasError(
                state.AcquireIoBackend(TestDiskId, TestClientId, id)));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, TestClientId, id).GetCode(),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, TestClientId, id).GetCode(),
            S_ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(
            state.AcquireIoBackend(TestDiskId, TestClientId, id)
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto second = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(second.GetSessionId() != id);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume(TestDiskId, TestClientId, id).GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(!HasError(state.AcquireIoBackend(
            TestDiskId,
            TestClientId,
            second.GetSessionId())));
    }

    Y_UNIT_TEST(ShouldRevokeSessionOnStopAndPartitionReplacement)
    {
        TFrontendState state;
        const auto registration =
            RegisterTestVolume(state, MakeTestVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        state.Start();
        const auto first = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(first));
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(MakeTestMountRequest()).GetSessionId(),
            first.GetSessionId());
        state.Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            state
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    first.GetSessionId())
                .GetError()
                .GetCode(),
            E_REJECTED);
        state.Stop();
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    first.GetSessionId())
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto second = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(second.GetSessionId() != first.GetSessionId());
        auto invalid = MakeTestVolumeConfig();
        invalid.SetBlockSize(1);
        UNIT_ASSERT(HasError(RegisterTestVolume(state, invalid)));
        UNIT_ASSERT(!HasError(state.AcquireIoBackend(
            TestDiskId,
            TestClientId,
            second.GetSessionId())));
        const auto replacement =
            RegisterTestVolume(state, MakeTestVolumeConfig());
        UNIT_ASSERT(!HasError(replacement));
        UNIT_ASSERT_VALUES_EQUAL(
            state
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    second.GetSessionId())
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto third = state.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(third));
        UNIT_ASSERT(third.GetSessionId() != second.GetSessionId());
        state.UnregisterVolume(registration.GetResult());
        UNIT_ASSERT(!HasError(state.AcquireIoBackend(
            TestDiskId,
            TestClientId,
            third.GetSessionId())));
        state.UnregisterVolume(replacement.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    third.GetSessionId())
                .GetError()
                .GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    third.GetSessionId())
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        TFrontendState recreated;
        recreated.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            recreated
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    third.GetSessionId())
                .GetError()
                .GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(recreated, MakeTestVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            recreated
                .AcquireIoBackend(
                    TestDiskId,
                    TestClientId,
                    third.GetSessionId())
                .GetError()
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto fourth = recreated.MountVolume(MakeTestMountRequest());
        UNIT_ASSERT(!HasError(fourth));
        UNIT_ASSERT(fourth.GetSessionId() != third.GetSessionId());
    }

    Y_UNIT_TEST(ShouldReadConsistentSessionSnapshotsDuringControlChanges)
    {
        TFrontendState state;
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        state.Start();
        auto request = MakeTestMountRequest();
        const auto first = state.MountVolume(request);
        UNIT_ASSERT(!HasError(first));
        const auto oldId = first.GetSessionId();
        request.MutableHeaders()->SetClientId("client2");
        std::barrier phase(2);
        std::atomic<ui32> failures = 0;
        std::thread reader(
            [&]
            {
                for (size_t iteration = 0; iteration != 128; ++iteration) {
                    phase.arrive_and_wait();
                    for (size_t check = 0; check != 128; ++check) {
                        // Client2 must never be combined with Client1's old
                        // token, even while admission, registration and session
                        // change.
                        const auto error =
                            state.AcquireIoBackend(TestDiskId, "client2", oldId)
                                .GetError();
                        if (error.GetCode() != E_BS_INVALID_SESSION &&
                            error.GetCode() != E_REJECTED)
                        {
                            ++failures;
                        }
                    }
                    phase.arrive_and_wait();
                }
            });
        for (size_t iteration = 0; iteration != 128; ++iteration) {
            phase.arrive_and_wait();
            state.Stop();
            if (HasError(RegisterTestVolume(state, MakeTestVolumeConfig()))) {
                ++failures;
            }
            state.Start();
            if (HasError(state.MountVolume(request))) {
                ++failures;
            }
            phase.arrive_and_wait();
        }
        reader.join();
        UNIT_ASSERT_VALUES_EQUAL(failures.load(), 0);
    }

    Y_UNIT_TEST(ShouldSerializeConcurrentMountsAndStop)
    {
        TFrontendState state;
        UNIT_ASSERT(
            !HasError(RegisterTestVolume(state, MakeTestVolumeConfig())));
        const auto request = MakeTestMountRequest();
        for (size_t iteration = 0; iteration != 32; ++iteration) {
            state.Start();
            std::barrier start(3);
            NCompatProto::TMountVolumeResponse first;
            NCompatProto::TMountVolumeResponse second;
            std::thread a(
                [&]
                {
                    start.arrive_and_wait();
                    first = state.MountVolume(request);
                });
            std::thread b(
                [&]
                {
                    start.arrive_and_wait();
                    second = state.MountVolume(request);
                });
            start.arrive_and_wait();
            a.join();
            b.join();
            UNIT_ASSERT(!HasError(first));
            UNIT_ASSERT(!HasError(second));
            UNIT_ASSERT_VALUES_EQUAL(
                first.GetSessionId(),
                second.GetSessionId());
            state.Stop();

            state.Start();
            std::thread mount(
                [&]
                {
                    start.arrive_and_wait();
                    first = state.MountVolume(request);
                });
            std::thread stop(
                [&]
                {
                    start.arrive_and_wait();
                    state.Stop();
                });
            start.arrive_and_wait();
            mount.join();
            stop.join();
            UNIT_ASSERT(
                first.GetError().GetCode() == S_OK ||
                first.GetError().GetCode() == E_REJECTED);
            UNIT_ASSERT_VALUES_EQUAL(
                state
                    .AcquireIoBackend(
                        TestDiskId,
                        TestClientId,
                        first.GetSessionId())
                    .GetError()
                    .GetCode(),
                E_REJECTED);
            state.Start();
            UNIT_ASSERT_VALUES_EQUAL(
                state
                    .AcquireIoBackend(
                        TestDiskId,
                        TestClientId,
                        first.GetSessionId())
                    .GetError()
                    .GetCode(),
                E_BS_INVALID_SESSION);
            state.Stop();
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore
