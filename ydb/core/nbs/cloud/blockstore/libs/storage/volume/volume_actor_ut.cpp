#include "volume.h"

#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct_tablet/partition_direct.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/volume.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/storage/core/protos/media.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

namespace NYdb::NBS::NStorage {

namespace {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

using TNbs1Service = NNbs1CompatApi::NBlockStore::TEvService;
using TNbs1Volume = NNbs1CompatApi::NBlockStore::TEvVolume;

constexpr ui64 DefaultVolumeBlockCount = 32768;
constexpr ui64 DefaultStripeSize = 512_KB;
constexpr ui64 DefaultVChunkSize = NBlockStore::MaxVChunkSize;
constexpr ui32 StorageMediaKindSsdDirectMirror3Of5Group = 10;
const TString DDiskPoolName = "ddp1";
const TString DiskId = "test-volume";
const ui64 PartitionTabletId = MakeTabletID(1, 0, 1);
const ui64 VolumeTabletId = MakeTabletID(1, 0, 2);

////////////////////////////////////////////////////////////////////////////////

// The environment helpers are a compact copy of the ones in
// partition_direct_ut.cpp.

// The NBS service the partition tablet needs, stopped on scope exit.
struct TScopedNbsService: TDisableCopyMove
{
    explicit TScopedNbsService(const NKikimrConfig::TNbsConfig& nbsConfig)
    {
        NBlockStore::CreateNbsService(nbsConfig);
        NBlockStore::StartNbsService();
    }

    ~TScopedNbsService()
    {
        NBlockStore::StopNbsService();
    }
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] NKikimrConfig::TNbsConfig CreateNbsConfig()
{
    NKikimrConfig::TNbsConfig nbsConfig;
    auto* storageConfig = nbsConfig.MutableNbsStorageConfig();
    storageConfig->SetDDiskPoolName(DDiskPoolName);
    storageConfig->SetPersistentBufferDDiskPoolName(DDiskPoolName);
    storageConfig->SetWriteMode(
        NBlockStore::GetProtoWriteMode(NBlockStore::EWriteMode::DirectWrite));
    storageConfig->SetVChunkSize(DefaultVChunkSize);
    storageConfig->SetStripeSize(DefaultStripeSize);
    storageConfig->SetWriteHedgingDelay(TDuration::Seconds(1).MicroSeconds());
    return nbsConfig;
}

// Defines the DDisk pool the partition allocates from and starts the NBS
// service.
[[nodiscard]] std::unique_ptr<TScopedNbsService> SetupStorage(
    TEnvironmentSetup& env)
{
    env.CreateBoxAndPool();
    env.Sim(TDuration::Seconds(30));

    NKikimrBlobStorage::TConfigRequest request;
    auto* cmd = request.AddCommand()->MutableDefineDDiskPool();
    cmd->SetBoxId(1);
    cmd->SetName(DDiskPoolName);
    auto* geometry = cmd->MutableGeometry();
    geometry->SetRealmLevelBegin(10);
    geometry->SetRealmLevelEnd(20);
    geometry->SetDomainLevelBegin(10);
    geometry->SetDomainLevelEnd(40);
    geometry->SetNumFailRealms(1);
    geometry->SetNumFailDomainsPerFailRealm(5);
    geometry->SetNumVDisksPerFailDomain(1);
    cmd->AddPDiskFilter()->AddProperty()->SetType(
        NKikimrBlobStorage::EPDiskType::ROT);
    cmd->SetNumDDiskGroups(3);
    const auto response = env.Invoke(request);
    UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

    return std::make_unique<TScopedNbsService>(CreateNbsConfig());
}

NKikimrBlockStore::TVolumeConfig CreateVolumeConfig(
    ui64 blockCount = DefaultVolumeBlockCount)
{
    NKikimrBlockStore::TVolumeConfig volumeConfig;
    volumeConfig.SetDiskId(DiskId);
    volumeConfig.SetBlockSize(NBlockStore::DefaultBlockSize);
    volumeConfig.SetStoragePoolName(DDiskPoolName);
    volumeConfig.SetStorageMediaKind(StorageMediaKindSsdDirectMirror3Of5Group);
    volumeConfig.SetVersion(1);
    volumeConfig.SetTabletVersion(3);
    volumeConfig.AddPartitions()->SetBlockCount(blockCount);
    return volumeConfig;
}

// Runs the tablet on the controller node and waits until it boots.
void BootTablet(
    TEnvironmentSetup& env,
    ui64 tabletId,
    IActor* (*factory)(const TActorId&, TTabletStorageInfo*))
{
    env.Runtime->CreateTestBootstrapper(
        TTestActorSystem::CreateTestTabletInfo(
            tabletId,
            TTabletTypes::Unknown,
            env.Settings.Erasure.GetErasure(),
            env.GroupId,
            3),   // NumChannels
        factory,
        env.Settings.ControllerNodeId);

    bool booting = true;
    env.Runtime->Sim(
        [&] { return booting; },
        [&](IEventHandle& event)
        {
            if (event.GetTypeRewrite() == TEvTablet::EvBoot &&
                event.Get<TEvTablet::TEvBoot>()->TabletID == tabletId)
            {
                booting = false;
            }
        });
}

void BootPartitionAndVolume(TEnvironmentSetup& env)
{
    BootTablet(
        env,
        PartitionTabletId,
        &NBlockStore::NStorage::NPartitionDirect::CreatePartitionTablet);
    BootTablet(env, VolumeTabletId, &CreateVolumeTablet);
}

// Sends the request to the tablet over a pipe and returns the response.
template <typename TResponse, typename TRequest>
TAutoPtr<TEventHandle<TResponse>> SendToTablet(
    TEnvironmentSetup& env,
    ui64 tabletId,
    std::unique_ptr<TRequest> request)
{
    const TActorId edge = env.Runtime->AllocateEdgeActor(
        env.Settings.ControllerNodeId,
        __FILE__,
        __LINE__);

    env.Runtime->SendToPipe(
        tabletId,
        edge,
        request.release(),
        0,
        TTestActorSystem::GetPipeConfigWithRetries());

    auto response = env.WaitForEdgeActorEvent<TResponse>(edge);
    UNIT_ASSERT(response);
    return response;
}

NKikimrBlockStore::TUpdateVolumeConfigResponse UpdateVolumeConfig(
    TEnvironmentSetup& env,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig,
    ui64 txId)
{
    auto request =
        std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
    request->Record.MutableVolumeConfig()->CopyFrom(volumeConfig);
    request->Record.SetTxId(txId);
    auto* partition = request->Record.AddPartitions();
    partition->SetPartitionId(0);
    partition->SetTabletId(PartitionTabletId);

    const auto response =
        SendToTablet<NKikimr::TEvBlockStore::TEvUpdateVolumeConfigResponse>(
            env,
            VolumeTabletId,
            std::move(request));
    return response->Get()->Record;
}

NNbs1CompatApi::NBlockStore::NProto::TStatVolumeResponse StatVolume(
    TEnvironmentSetup& env)
{
    auto request = std::make_unique<TNbs1Service::TEvStatVolumeRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetNoPartition(true);

    const auto response = SendToTablet<TNbs1Service::TEvStatVolumeResponse>(
        env,
        VolumeTabletId,
        std::move(request));
    return response->Get()->Record;
}

NNbs1CompatApi::NBlockStore::NProto::TWaitReadyResponse WaitReady(
    TEnvironmentSetup& env)
{
    auto request = std::make_unique<TNbs1Volume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(DiskId);

    const auto response = SendToTablet<TNbs1Volume::TEvWaitReadyResponse>(
        env,
        VolumeTabletId,
        std::move(request));
    return response->Get()->Record;
}

void CheckVolume(
    const NNbs1CompatApi::NBlockStore::NProto::TVolume& volume,
    ui64 blockCount)
{
    UNIT_ASSERT_VALUES_EQUAL(DiskId, volume.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(blockCount, volume.GetBlocksCount());
    UNIT_ASSERT_VALUES_EQUAL(
        NBlockStore::DefaultBlockSize,
        volume.GetBlockSize());
    UNIT_ASSERT_EQUAL(
        NNbs1CompatApi::NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP,
        volume.GetStorageMediaKind());
    UNIT_ASSERT_VALUES_EQUAL(1u, volume.GetConfigVersion());
    UNIT_ASSERT_VALUES_EQUAL(3u, volume.GetTabletVersion());
    UNIT_ASSERT_VALUES_EQUAL(1u, volume.GetPartitionsCount());
}

// Restarts the node hosting both tablets and boots them again.
void RestartTabletNode(
    TEnvironmentSetup& env,
    std::unique_ptr<TScopedNbsService>* scopedService)
{
    scopedService->reset();
    env.RestartNode(env.Settings.ControllerNodeId);
    env.Sim(TDuration::Seconds(1));
    *scopedService = std::make_unique<TScopedNbsService>(CreateNbsConfig());
    BootPartitionAndVolume(env);
    env.Sim(TDuration::Seconds(10));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDirectTest)
{
    Y_UNIT_TEST(ShouldRejectRequestsBeforeUpdateVolumeConfig)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        env.Runtime->SetLogPriority(
            NKikimrServices::NBS_VOLUME,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env);
        BootPartitionAndVolume(env);

        const auto statResponse = StatVolume(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            statResponse.GetError().GetCode(),
            FormatError(statResponse.GetError()));

        const auto waitReadyResponse = WaitReady(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            waitReadyResponse.GetError().GetCode(),
            FormatError(waitReadyResponse.GetError()));
    }

    Y_UNIT_TEST(ShouldForwardStatVolumeAndWaitReadyToPartition)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        env.Runtime->SetLogPriority(
            NKikimrServices::NBS_VOLUME,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env);
        BootPartitionAndVolume(env);

        const auto updateResponse =
            UpdateVolumeConfig(env, CreateVolumeConfig(), 1);
        UNIT_ASSERT_EQUAL(NKikimrBlockStore::OK, updateResponse.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(1u, updateResponse.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL(VolumeTabletId, updateResponse.GetOrigin());

        // The partition answers WaitReady once it has applied the config.
        const auto waitReadyResponse = WaitReady(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            waitReadyResponse.GetError().GetCode(),
            FormatError(waitReadyResponse.GetError()));
        CheckVolume(waitReadyResponse.GetVolume(), DefaultVolumeBlockCount);

        // The partition completes its startup requests to the DDisks.
        env.Sim(TDuration::Seconds(10));

        const auto statResponse = StatVolume(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            statResponse.GetError().GetCode(),
            FormatError(statResponse.GetError()));
        CheckVolume(statResponse.GetVolume(), DefaultVolumeBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0u, statResponse.ClientsSize());
        UNIT_ASSERT(!statResponse.GetIsVolumeOperationRestricted());
        UNIT_ASSERT(!statResponse.HasStats());
    }

    Y_UNIT_TEST(ShouldReplyOkToRepeatedUpdateVolumeConfig)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};

        auto scopedService = SetupStorage(env);
        BootPartitionAndVolume(env);

        const auto volumeConfig = CreateVolumeConfig();
        {
            const auto response = UpdateVolumeConfig(env, volumeConfig, 1);
            UNIT_ASSERT_EQUAL(NKikimrBlockStore::OK, response.GetStatus());
        }

        // The partition answers WaitReady once it has applied the config.
        {
            const auto response = WaitReady(env);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        // The partition completes its startup requests to the DDisks.
        env.Sim(TDuration::Seconds(10));

        for (const ui64 txId: {1, 2}) {
            const auto response = UpdateVolumeConfig(env, volumeConfig, txId);
            UNIT_ASSERT_EQUAL(NKikimrBlockStore::OK, response.GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(txId, response.GetTxId());
            UNIT_ASSERT_VALUES_EQUAL(VolumeTabletId, response.GetOrigin());
        }

        const auto statResponse = StatVolume(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            statResponse.GetError().GetCode(),
            FormatError(statResponse.GetError()));
        CheckVolume(statResponse.GetVolume(), DefaultVolumeBlockCount);
    }

    Y_UNIT_TEST(ShouldForwardRequestsAfterRestart)
    {
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        env.Runtime->SetLogPriority(
            NKikimrServices::NBS_VOLUME,
            NActors::NLog::PRI_DEBUG);

        auto scopedService = SetupStorage(env);
        BootPartitionAndVolume(env);

        const auto updateResponse =
            UpdateVolumeConfig(env, CreateVolumeConfig(), 1);
        UNIT_ASSERT_EQUAL(NKikimrBlockStore::OK, updateResponse.GetStatus());

        // The partition answers WaitReady once it has applied the config.
        {
            const auto response = WaitReady(env);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        RestartTabletNode(env, &scopedService);

        // The partition tablet id is read from the volume tablet's local DB.
        const auto waitReadyResponse = WaitReady(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            waitReadyResponse.GetError().GetCode(),
            FormatError(waitReadyResponse.GetError()));
        CheckVolume(waitReadyResponse.GetVolume(), DefaultVolumeBlockCount);

        const auto statResponse = StatVolume(env);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            statResponse.GetError().GetCode(),
            FormatError(statResponse.GetError()));
        CheckVolume(statResponse.GetVolume(), DefaultVolumeBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(0u, statResponse.ClientsSize());
    }
}

}   // namespace NYdb::NBS::NStorage
