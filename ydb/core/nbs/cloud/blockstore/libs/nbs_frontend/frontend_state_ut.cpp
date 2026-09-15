#include "frontend_state.h"

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <atomic>
#include <barrier>
#include <thread>

namespace NYdb::NBS::NBlockStore {

namespace {

namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

NKikimrBlockStore::TVolumeConfig MakeVolumeConfig()
{
    NKikimrBlockStore::TVolumeConfig config;
    config.SetDiskId("disk1");
    config.SetBlockSize(4096);
    config.AddPartitions()->SetBlockCount(33554432);
    config.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
    config.SetVersion(42);
    config.SetProjectId("project");
    config.SetFolderId("folder");
    config.SetCloudId("cloud");
    return config;
}

NCompatProto::TMountVolumeRequest MakeMountRequest()
{
    NCompatProto::TMountVolumeRequest request;
    request.SetDiskId("disk1");
    request.MutableHeaders()->SetClientId("client1");
    return request;
}

}   // namespace

Y_UNIT_TEST_SUITE(TFrontendStateMetadataTest)
{
    Y_UNIT_TEST(ShouldCopyMetadataAndConvertClassicVolume)
    {
        TFrontendState state;
        auto config = MakeVolumeConfig();
        const auto registration = state.RegisterVolume(config);
        UNIT_ASSERT(!HasError(registration));
        UNIT_ASSERT(!registration.GetResult().empty());

        // Publication owns a copy; actor-side mutation cannot change it.
        config.SetDiskId("changed");
        config.MutablePartitions(0)->SetBlockCount(1);
        state.Start();
        const auto result = state.GetVolume("disk1");
        UNIT_ASSERT(!HasError(result));
        const auto& volume = result.GetResult();
        UNIT_ASSERT_VALUES_EQUAL(volume.GetDiskId(), "disk1");
        UNIT_ASSERT_VALUES_EQUAL(volume.GetBlockSize(), 4096);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetBlocksCount(), 33554432);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetPartitionsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(volume.GetStorageMediaKind()),
            static_cast<ui32>(NNbs1CompatApi::NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT_VALUES_EQUAL(volume.GetConfigVersion(), 42);
        UNIT_ASSERT_VALUES_EQUAL(volume.GetProjectId(), "project");
        UNIT_ASSERT_VALUES_EQUAL(volume.GetFolderId(), "folder");
        UNIT_ASSERT_VALUES_EQUAL(volume.GetCloudId(), "cloud");
        UNIT_ASSERT_VALUES_EQUAL(volume.DevicesSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(volume.MigrationsSize(), 0);
    }

    Y_UNIT_TEST(ShouldRejectInvalidMetadataWithoutReplacingRegistration)
    {
        TFrontendState state;
        const auto good = MakeVolumeConfig();
        UNIT_ASSERT(!HasError(state.RegisterVolume(good)));
        state.Start();

        TVector<NKikimrBlockStore::TVolumeConfig> invalid(7, good);
        invalid[0].ClearDiskId();
        invalid[1].ClearPartitions();
        invalid[2].AddPartitions()->SetBlockCount(1);
        invalid[3].SetBlockSize(8192);
        invalid[4].MutablePartitions(0)->SetBlockCount(0);
        invalid[5].SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        invalid[6].SetDiskId("another-disk");
        for (const auto& config: invalid) {
            UNIT_ASSERT_VALUES_EQUAL(
                state.RegisterVolume(config).GetError().GetCode(),
                E_ARGUMENT);
            const auto volume = state.GetVolume("disk1");
            UNIT_ASSERT(!HasError(volume));
            UNIT_ASSERT_VALUES_EQUAL(
                volume.GetResult().GetBlocksCount(),
                33554432);
        }
    }

    Y_UNIT_TEST(ShouldGuardReplacementFromOldUnregister)
    {
        TFrontendState state;
        auto config = MakeVolumeConfig();
        const auto first = state.RegisterVolume(config);
        UNIT_ASSERT(!HasError(first));
        // The fixture size is not a frontend constant.
        config.MutablePartitions(0)->SetBlockCount(1024);
        const auto second = state.RegisterVolume(config);
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(first.GetResult() != second.GetResult());
        state.Start();
        state.UnregisterVolume(first.GetResult());
        const auto volume = state.GetVolume("disk1");
        UNIT_ASSERT(!HasError(volume));
        UNIT_ASSERT_VALUES_EQUAL(volume.GetResult().GetBlocksCount(), 1024);
        state.UnregisterVolume(second.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_NOT_FOUND);
        state.UnregisterVolume(second.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldPreserveRegistrationAcrossStopStart)
    {
        TFrontendState state;
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT(!HasError(state.CheckAcceptingRequests()));
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_NOT_FOUND);

        const auto registration = state.RegisterVolume(MakeVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        state.Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            state.CheckAcceptingRequests().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT(!HasError(state.GetVolume("disk1")));
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
            state.GetVolume("disk1").GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldNotReuseRegistrationFromPreviousRuntime)
    {
        TString oldRegistration;
        {
            TFrontendState oldState;
            const auto registration =
                oldState.RegisterVolume(MakeVolumeConfig());
            UNIT_ASSERT(!HasError(registration));
            oldRegistration = registration.GetResult();
        }
        TFrontendState state;
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.GetVolume("disk1").GetError().GetCode(),
            E_NOT_FOUND);
        const auto registration = state.RegisterVolume(MakeVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        UNIT_ASSERT(registration.GetResult() != oldRegistration);
        state.UnregisterVolume(oldRegistration);
        UNIT_ASSERT(!HasError(state.GetVolume("disk1")));
    }
}

Y_UNIT_TEST_SUITE(TFrontendStateSessionTest)
{
    Y_UNIT_TEST(ShouldMountIdempotentlyAndIgnoreNonIdentityParameters)
    {
        TFrontendState state;
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        state.Start();
        auto request = MakeMountRequest();
        const auto first = state.MountVolume(request);
        UNIT_ASSERT(!HasError(first));
        UNIT_ASSERT(!first.GetSessionId().empty());
        UNIT_ASSERT_VALUES_EQUAL(first.GetInactiveClientsTimeout(), 0);
        UNIT_ASSERT_VALUES_EQUAL(
            first.GetVolume().SerializeAsString(),
            state.GetVolume("disk1").GetResult().SerializeAsString());

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
        UNIT_ASSERT(!HasError(
            state.ValidateIoSession("disk1", "client1", first.GetSessionId())));
        UNIT_ASSERT(!HasError(
            state.UnmountVolume("disk1", "client1", first.GetSessionId())));
        const auto next = state.MountVolume(request);
        UNIT_ASSERT(!HasError(next));
        UNIT_ASSERT(next.GetSessionId() != first.GetSessionId());
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMountParametersWithoutChangingSession)
    {
        TVector<NCompatProto::TMountVolumeRequest> invalid(
            15,
            MakeMountRequest());
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
            UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
            state.Start();
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                state.UnmountVolume("disk1", "client1", "unknown").GetCode(),
                S_ALREADY);
            const auto first = state.MountVolume(MakeMountRequest());
            UNIT_ASSERT(!HasError(first));
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                state.MountVolume(MakeMountRequest()).GetSessionId(),
                first.GetSessionId());
        }
    }

    Y_UNIT_TEST(ShouldApplyMountErrorPriority)
    {
        TFrontendState state;
        auto request = MakeMountRequest();
        request.MutableHeaders()->ClearClientId();
        request.SetMountSeqNumber(1);
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(request).GetError().GetCode(),
            E_ARGUMENT);
        const auto first = state.MountVolume(MakeMountRequest());
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
        UNIT_ASSERT(!HasError(
            state.ValidateIoSession("disk1", "client1", first.GetSessionId())));
    }

    Y_UNIT_TEST(ShouldValidateUnmountAndIoWithoutRevokingAnotherSession)
    {
        TFrontendState state;
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("", "", "").GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("", "", "").GetCode(),
            E_REJECTED);
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "", "").GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "", "").GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "", "unknown").GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "client1", "").GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "client1", "unknown").GetCode(),
            S_ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", "unknown").GetCode(),
            E_BS_INVALID_SESSION);

        const auto first = state.MountVolume(MakeMountRequest());
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
            {"", "client1", id, E_NOT_FOUND},
            {"other-disk", "", "", E_NOT_FOUND},
            {"disk1", "", id, E_BS_INVALID_SESSION},
            {"disk1", "client1", "", E_BS_INVALID_SESSION},
            {"disk1", "client2", id, E_BS_INVALID_SESSION},
            {"disk1", "client1", "old-token", E_BS_INVALID_SESSION},
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
                    .ValidateIoSession(
                        request.DiskId,
                        request.ClientId,
                        request.SessionId)
                    .GetCode(),
                request.Error);
            UNIT_ASSERT(
                !HasError(state.ValidateIoSession("disk1", "client1", id)));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "client1", id).GetCode(),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "client1", id).GetCode(),
            S_ALREADY);
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", id).GetCode(),
            E_BS_INVALID_SESSION);
        const auto second = state.MountVolume(MakeMountRequest());
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(second.GetSessionId() != id);
        UNIT_ASSERT_VALUES_EQUAL(
            state.UnmountVolume("disk1", "client1", id).GetCode(),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT(!HasError(state.ValidateIoSession(
            "disk1",
            "client1",
            second.GetSessionId())));
    }

    Y_UNIT_TEST(ShouldRevokeSessionOnStopAndPartitionReplacement)
    {
        TFrontendState state;
        const auto registration = state.RegisterVolume(MakeVolumeConfig());
        UNIT_ASSERT(!HasError(registration));
        state.Start();
        const auto first = state.MountVolume(MakeMountRequest());
        UNIT_ASSERT(!HasError(first));
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.MountVolume(MakeMountRequest()).GetSessionId(),
            first.GetSessionId());
        state.Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", first.GetSessionId())
                .GetCode(),
            E_REJECTED);
        state.Stop();
        state.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", first.GetSessionId())
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto second = state.MountVolume(MakeMountRequest());
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(second.GetSessionId() != first.GetSessionId());
        auto invalid = MakeVolumeConfig();
        invalid.SetBlockSize(1);
        UNIT_ASSERT(HasError(state.RegisterVolume(invalid)));
        UNIT_ASSERT(!HasError(state.ValidateIoSession(
            "disk1",
            "client1",
            second.GetSessionId())));
        const auto replacement = state.RegisterVolume(MakeVolumeConfig());
        UNIT_ASSERT(!HasError(replacement));
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", second.GetSessionId())
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto third = state.MountVolume(MakeMountRequest());
        UNIT_ASSERT(!HasError(third));
        UNIT_ASSERT(third.GetSessionId() != second.GetSessionId());
        state.UnregisterVolume(registration.GetResult());
        UNIT_ASSERT(!HasError(
            state.ValidateIoSession("disk1", "client1", third.GetSessionId())));
        state.UnregisterVolume(replacement.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", third.GetSessionId())
                .GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            state.ValidateIoSession("disk1", "client1", third.GetSessionId())
                .GetCode(),
            E_BS_INVALID_SESSION);
        TFrontendState recreated;
        recreated.Start();
        UNIT_ASSERT_VALUES_EQUAL(
            recreated
                .ValidateIoSession("disk1", "client1", third.GetSessionId())
                .GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(recreated.RegisterVolume(MakeVolumeConfig())));
        UNIT_ASSERT_VALUES_EQUAL(
            recreated
                .ValidateIoSession("disk1", "client1", third.GetSessionId())
                .GetCode(),
            E_BS_INVALID_SESSION);
        const auto fourth = recreated.MountVolume(MakeMountRequest());
        UNIT_ASSERT(!HasError(fourth));
        UNIT_ASSERT(fourth.GetSessionId() != third.GetSessionId());
    }

    Y_UNIT_TEST(ShouldReadConsistentSessionSnapshotsDuringControlChanges)
    {
        TFrontendState state;
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        state.Start();
        auto request = MakeMountRequest();
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
                            state.ValidateIoSession("disk1", "client2", oldId);
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
            if (HasError(state.RegisterVolume(MakeVolumeConfig()))) {
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
        UNIT_ASSERT(!HasError(state.RegisterVolume(MakeVolumeConfig())));
        const auto request = MakeMountRequest();
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
                    .ValidateIoSession("disk1", "client1", first.GetSessionId())
                    .GetCode(),
                E_REJECTED);
            state.Start();
            UNIT_ASSERT_VALUES_EQUAL(
                state
                    .ValidateIoSession("disk1", "client1", first.GetSessionId())
                    .GetCode(),
                E_BS_INVALID_SESSION);
            state.Stop();
        }
    }
}

}   // namespace NYdb::NBS::NBlockStore
