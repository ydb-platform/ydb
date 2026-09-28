#include "frontend_test.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_test.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <barrier>
#include <thread>

namespace NYdb::NBS::NBlockStore {

namespace {

using namespace NTests;
namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

NCompatProto::TMountVolumeResponse Mount(
    const INbsBlockStoreFacadePtr& facade,
    const NCompatProto::TMountVolumeRequest& request)
{
    auto future = facade->MountVolume(
        MakeIntrusive<TCallContext>(),
        std::make_shared<NCompatProto::TMountVolumeRequest>(request));
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    return future.GetValueSync();
}

NProto::TError Unmount(
    const INbsBlockStoreFacadePtr& facade,
    const TString& diskId,
    const TString& clientId,
    const TString& sessionId)
{
    auto request = std::make_shared<NCompatProto::TUnmountVolumeRequest>();
    request->SetDiskId(diskId);
    request->SetSessionId(sessionId);
    request->MutableHeaders()->SetClientId(clientId);
    auto future = facade->UnmountVolume(MakeIntrusive<TCallContext>(), request);
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    return future.GetValueSync().GetError();
}

// Checks backend access and returned data for an otherwise valid I/O request.
ui32 ReadBlock(
    const INbsBlockStoreFacadePtr& facade,
    const TString& diskId,
    const TString& clientId,
    const TString& sessionId,
    ui32 blockSize,
    const ui32& readCalls)
{
    const ui32 callsBefore = readCalls;
    auto request = std::make_shared<NCompatProto::TReadBlocksRequest>();
    request->SetDiskId(diskId);
    request->SetSessionId(sessionId);
    request->SetBlockSize(blockSize);
    request->SetBlocksCount(1);
    request->MutableHeaders()->SetClientId(clientId);
    auto future = facade->ReadBlocks(MakeIntrusive<TCallContext>(), request);
    UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
    const auto response = future.GetValueSync();
    if (HasError(response)) {
        UNIT_ASSERT_VALUES_EQUAL(readCalls, callsBefore);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(readCalls, callsBefore + 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetBlocks().BuffersSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            response.GetBlocks().GetBuffers(0),
            TString(blockSize, 'x'));
    }
    return response.GetError().GetCode();
}

}   // namespace

Y_UNIT_TEST_SUITE(TFrontendRegistryTest)
{
    Y_UNIT_TEST(ShouldRejectRegistrationWithoutSessionControl)
    {
        ui32 readCalls = 0;
        TFrontendTestEnv env;
        const auto config = MakeTestVolumeConfig();
        UNIT_ASSERT(!HasError(RegisterTestVolume(env, config, &readCalls)));
        env.Facade->Start();
        const auto mounted = Mount(env.Facade, MakeTestMountRequest());
        UNIT_ASSERT(!HasError(mounted));

        auto state = NStorage::NPartitionDirect::TPartitionSession::Create(
            config,
            std::make_shared<TTestStorage>(),
            MakeTestIoConfig(config));
        UNIT_ASSERT(!HasError(state));
        const auto registration =
            env.Facade->RegisterVolume(state.ExtractResult(), {});
        UNIT_ASSERT_VALUES_EQUAL(registration.GetError().GetCode(), E_ARGUMENT);

        // A rejected replacement must preserve the original control and I/O
        // path.
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, MakeTestMountRequest()).GetSessionId(),
            mounted.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                mounted.GetSessionId(),
                config.GetBlockSize(),
                readCalls),
            S_OK);
    }

    Y_UNIT_TEST(ShouldCopyMetadataAndConvertClassicVolume)
    {
        TFrontendTestEnv env;
        // An already obtained facade must see registrations published after
        // Start.
        const auto blockStore = env.Facade;
        env.Facade->Start();
        auto config = MakeTestVolumeConfig();
        UNIT_ASSERT(!HasError(RegisterTestVolume(env, config)));
        const auto expected = config;
        config.SetDiskId("changed");
        config.MutablePartitions(0)->SetBlockCount(1);
        const auto future = blockStore->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NCompatProto::TMountVolumeRequest>(
                MakeTestMountRequest()));
        UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
        const auto mounted = future.GetValueSync();
        UNIT_ASSERT(!HasError(mounted));
        UNIT_ASSERT(!mounted.GetSessionId().empty());
        const auto& volume = mounted.GetVolume();
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
        UNIT_ASSERT_VALUES_EQUAL(mounted.GetInactiveClientsTimeout(), 0);
    }

    Y_UNIT_TEST(ShouldKeepOneIndependentSessionPerDisk)
    {
        ui32 readCalls = 0;
        TFrontendTestEnv env;
        UNIT_ASSERT(!HasError(
            RegisterTestVolume(env, MakeTestVolumeConfig(), &readCalls)));
        auto other = MakeTestVolumeConfig(8192, 1024);
        other.SetDiskId("disk2");
        const auto registration = RegisterTestVolume(env, other, &readCalls);
        UNIT_ASSERT(!HasError(registration));
        env.Facade->Start();
        auto request = MakeTestMountRequest();
        const auto first = Mount(env.Facade, request);
        UNIT_ASSERT(!HasError(first));
        request.SetDiskId("disk2");
        request.MutableHeaders()->SetClientId("client2");
        const auto second = Mount(env.Facade, request);
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(first.GetSessionId() != second.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(second.GetVolume().GetBlocksCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(second.GetVolume().GetBlockSize(), 8192);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                first.GetSessionId(),
                first.GetVolume().GetBlockSize(),
                readCalls),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                "disk2",
                "client2",
                second.GetSessionId(),
                other.GetBlockSize(),
                readCalls),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                "disk2",
                TestClientId,
                first.GetSessionId(),
                other.GetBlockSize(),
                readCalls),
            E_BS_INVALID_SESSION);
        UNIT_ASSERT_VALUES_EQUAL(
            Unmount(env.Facade, "disk2", TestClientId, first.GetSessionId())
                .GetCode(),
            E_BS_INVALID_SESSION);
        request.MutableHeaders()->SetClientId(TestClientId);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        UNIT_ASSERT_VALUES_EQUAL(
            Unmount(env.Facade, TestDiskId, TestClientId, first.GetSessionId())
                .GetCode(),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                "disk2",
                "client2",
                second.GetSessionId(),
                other.GetBlockSize(),
                readCalls),
            S_OK);
        env.UnregisterVolume("disk2", registration.GetResult());
        UNIT_ASSERT(!HasError(Mount(env.Facade, MakeTestMountRequest())));
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldPreserveSessionAcrossFrontendStopStart)
    {
        ui32 readCalls = 0;
        TFrontendTestEnv env;
        const auto registration =
            RegisterTestVolume(env, MakeTestVolumeConfig(), &readCalls)
                .ExtractResult();
        auto request = MakeTestMountRequest();
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_REJECTED);
        env.Facade->Start();
        const auto first = Mount(env.Facade, request);
        UNIT_ASSERT(!HasError(first));
        env.Facade->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                first.GetSessionId(),
                first.GetVolume().GetBlockSize(),
                readCalls),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_REJECTED);
        UNIT_ASSERT_VALUES_EQUAL(
            Unmount(env.Facade, TestDiskId, TestClientId, first.GetSessionId())
                .GetCode(),
            E_REJECTED);
        env.Facade->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                first.GetSessionId(),
                first.GetVolume().GetBlockSize(),
                readCalls),
            S_OK);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetSessionId(),
            first.GetSessionId());
        // Registry changes remain effective while admission is closed, but
        // must not reopen it.
        env.Facade->Stop();
        env.UnregisterVolume(TestDiskId, registration);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_REJECTED);
        auto other = MakeTestVolumeConfig();
        other.SetDiskId("disk2");
        UNIT_ASSERT(!HasError(RegisterTestVolume(env, other)));
        request.SetDiskId(other.GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_REJECTED);
        env.Facade->Start();
        UNIT_ASSERT(!HasError(Mount(env.Facade, request)));
        request.SetDiskId(TestDiskId);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_NOT_FOUND);
    }

    Y_UNIT_TEST(ShouldRevokeReplacedSessionAndIgnoreStaleUnregister)
    {
        ui32 readCalls = 0;
        TFrontendTestEnv env;
        auto config = MakeTestVolumeConfig();
        const auto firstRegistration =
            RegisterTestVolume(env, config, &readCalls).ExtractResult();
        env.Facade->Start();
        const auto first = Mount(env.Facade, MakeTestMountRequest());
        UNIT_ASSERT(!HasError(first));
        auto invalid = config;
        invalid.SetStorageMediaKind(NProto::STORAGE_MEDIA_HDD);
        UNIT_ASSERT_VALUES_EQUAL(
            RegisterTestVolume(env, invalid).GetError().GetCode(),
            E_ARGUMENT);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, MakeTestMountRequest()).GetSessionId(),
            first.GetSessionId());
        config.MutablePartitions(0)->SetBlockCount(1024);
        const auto replacement =
            RegisterTestVolume(env, config, &readCalls).ExtractResult();
        UNIT_ASSERT(replacement != firstRegistration);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                first.GetSessionId(),
                config.GetBlockSize(),
                readCalls),
            E_BS_INVALID_SESSION);
        const auto second = Mount(env.Facade, MakeTestMountRequest());
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT(second.GetSessionId() != first.GetSessionId());
        env.UnregisterVolume(TestDiskId, firstRegistration);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, MakeTestMountRequest()).GetSessionId(),
            second.GetSessionId());
        UNIT_ASSERT_VALUES_EQUAL(second.GetVolume().GetBlocksCount(), 1024);
        env.UnregisterVolume(TestDiskId, replacement);
        env.UnregisterVolume(TestDiskId, replacement);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, MakeTestMountRequest()).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(
            RegisterTestVolume(env, MakeTestVolumeConfig(), &readCalls)));
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                second.GetSessionId(),
                config.GetBlockSize(),
                readCalls),
            E_BS_INVALID_SESSION);
    }

    Y_UNIT_TEST(ShouldIgnoreNonIdentityMountParameters)
    {
        TFrontendTestEnv env;
        UNIT_ASSERT(!HasError(RegisterTestVolume(env, MakeTestVolumeConfig())));
        env.Facade->Start();
        auto request = MakeTestMountRequest();
        const auto first = Mount(env.Facade, request);
        UNIT_ASSERT(!HasError(first));
        request.SetInstanceId("another-vm");
        request.SetIpcType(NCompatProto::IPC_VHOST);
        request.SetClientVersionInfo("another-version");
        request.SetForceRemoteBinding(true);
        request.SetVolumeMountMode(NCompatProto::VOLUME_MOUNT_REMOTE);
        request.MutableEncryptionSpec()->SetMode(NCompatProto::NO_ENCRYPTION);
        request.MutableHeaders()->SetRequestId(42);
        request.MutableHeaders()->SetTraceId("another-trace");
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetSessionId(),
            first.GetSessionId());
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedMountParametersWithoutChangingSession)
    {
        TVector<NCompatProto::TMountVolumeRequest> invalid;
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetVolumeAccessMode(
            NCompatProto::VOLUME_ACCESS_READ_ONLY);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetVolumeAccessMode(NCompatProto::VOLUME_ACCESS_REPAIR);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetVolumeAccessMode(
            NCompatProto::VOLUME_ACCESS_USER_READ_ONLY);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetVolumeMountMode(
            static_cast<NCompatProto::EVolumeMountMode>(42));
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetMountSeqNumber(1);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetFillSeqNumber(1);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetFillGeneration(1);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetMountFlags(1);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetThrottlingDisabled(true);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetToken("token");
        invalid.push_back(MakeTestMountRequest());
        invalid.back().SetForceDisableEncryption(true);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().MutableEncryptionSpec()->SetMode(
            NCompatProto::ENCRYPTION_AES_XTS);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().MutableEncryptionSpec()->SetMode(
            NCompatProto::ENCRYPTION_WITH_ROOT_KMS_PROVIDED_KEY);
        invalid.push_back(MakeTestMountRequest());
        invalid.back().MutableEncryptionSpec()->MutableKeyPath();
        // Even an explicitly supplied empty key parameter is unsupported.
        invalid.push_back(MakeTestMountRequest());
        invalid.back().MutableEncryptionSpec()->SetKeyHash("");
        for (const auto& request: invalid) {
            TFrontendTestEnv env;
            UNIT_ASSERT(
                !HasError(RegisterTestVolume(env, MakeTestVolumeConfig())));
            env.Facade->Start();
            UNIT_ASSERT_VALUES_EQUAL(
                Mount(env.Facade, request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                Unmount(env.Facade, TestDiskId, TestClientId, "unknown")
                    .GetCode(),
                S_ALREADY);
            const auto first = Mount(env.Facade, MakeTestMountRequest());
            UNIT_ASSERT(!HasError(first));
            UNIT_ASSERT_VALUES_EQUAL(
                Mount(env.Facade, request).GetError().GetCode(),
                E_NOT_IMPLEMENTED);
            UNIT_ASSERT_VALUES_EQUAL(
                Mount(env.Facade, MakeTestMountRequest()).GetSessionId(),
                first.GetSessionId());
        }
    }

    Y_UNIT_TEST(ShouldApplyMountErrorPriority)
    {
        ui32 readCalls = 0;
        TFrontendTestEnv env;
        auto request = MakeTestMountRequest();
        request.MutableHeaders()->ClearClientId();
        request.SetMountSeqNumber(1);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_REJECTED);
        env.Facade->Start();
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT(!HasError(
            RegisterTestVolume(env, MakeTestVolumeConfig(), &readCalls)));
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_ARGUMENT);
        const auto first = Mount(env.Facade, MakeTestMountRequest());
        UNIT_ASSERT(!HasError(first));
        request.MutableHeaders()->SetClientId("another-client");
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_NOT_IMPLEMENTED);
        request.SetMountSeqNumber(0);
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_BS_MOUNT_CONFLICT);
        request.SetDiskId("");
        UNIT_ASSERT_VALUES_EQUAL(
            Mount(env.Facade, request).GetError().GetCode(),
            E_NOT_FOUND);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadBlock(
                env.Facade,
                TestDiskId,
                TestClientId,
                first.GetSessionId(),
                first.GetVolume().GetBlockSize(),
                readCalls),
            S_OK);
    }

    Y_UNIT_TEST(ShouldSerializeConcurrentMountsOnPartitionActor)
    {
        TFrontendTestEnv env;
        UNIT_ASSERT(!HasError(RegisterTestVolume(env, MakeTestVolumeConfig())));
        env.Facade->Start();
        const auto request = MakeTestMountRequest();
        std::barrier start(3);
        NCompatProto::TMountVolumeResponse first;
        NCompatProto::TMountVolumeResponse second;
        std::thread a(
            [&]
            {
                start.arrive_and_wait();
                first = Mount(env.Facade, request);
            });
        std::thread b(
            [&]
            {
                start.arrive_and_wait();
                second = Mount(env.Facade, request);
            });
        start.arrive_and_wait();
        a.join();
        b.join();
        UNIT_ASSERT(!HasError(first));
        UNIT_ASSERT(!HasError(second));
        UNIT_ASSERT_VALUES_EQUAL(first.GetSessionId(), second.GetSessionId());
    }
}

}   // namespace NYdb::NBS::NBlockStore
