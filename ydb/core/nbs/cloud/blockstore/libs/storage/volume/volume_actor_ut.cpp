#include "volume.h"

#include "volume_actor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/storage/api/volume.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NStorage {

using namespace NKikimr;

// Test-only read of the loaded partition tablet id.
class TVolumeActorTestAccessor final
{
public:
    // In-memory PartitionTabletId. 0 means the volume has not stored one.
    static ui64 PartitionTabletId(const TVolumeActor& actor)
    {
        return actor.PartitionTabletId;
    }
};

namespace {

////////////////////////////////////////////////////////////////////////////////

using TNbs1Service = NNbs1CompatApi::NBlockStore::TEvService;
using TNbs1Volume = NNbs1CompatApi::NBlockStore::TEvVolume;

const ui64 VolumeTabletId = MakeTabletID(0, 0, 1);

// Pipes open only after LoadState, which sends TEvTabletActive.
void BootVolumeTablet(TTestBasicRuntime& runtime)
{
    CreateTestBootstrapper(
        runtime,
        CreateTestTabletInfo(VolumeTabletId, TTabletTypes::Unknown),
        &CreateVolumeTablet);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
    options.FinalEvents.emplace_back(TEvTablet::EvTabletActive, 1);
    runtime.DispatchEvents(options);
}

const TVolumeActor& FindVolume(
    TTestBasicRuntime& runtime,
    const TActorId& actorId)
{
    IActor* actor = runtime.FindActor(actorId);
    UNIT_ASSERT(actor);
    const auto* volume = dynamic_cast<const TVolumeActor*>(actor);
    UNIT_ASSERT(volume);
    return *volume;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDirectTest)
{
    Y_UNIT_TEST(ShouldAnswerStatVolume)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        BootVolumeTablet(runtime);

        auto request = std::make_unique<TNbs1Service::TEvStatVolumeRequest>();
        request->Record.SetDiskId("test-volume");
        request->Record.SetNoPartition(true);

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(
            VolumeTabletId,
            edge,
            request.release(),
            0,   // nodeIndex
            NTabletPipe::TClientConfig(),
            TActorId(),   // clientId
            42);          // cookie

        TAutoPtr<IEventHandle> handle;
        const auto* response =
            runtime.GrabEdgeEvent<TNbs1Service::TEvStatVolumeResponse>(handle);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(42u, handle->Cookie);
        UNIT_ASSERT_C(
            !HasError(response->GetError()),
            FormatError(response->GetError()));
        UNIT_ASSERT_VALUES_EQUAL(0u, response->Record.ClientsSize());
    }

    Y_UNIT_TEST(ShouldAnswerWaitReady)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        BootVolumeTablet(runtime);

        auto request = std::make_unique<TNbs1Volume::TEvWaitReadyRequest>();
        request->Record.SetDiskId("test-volume");

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(
            VolumeTabletId,
            edge,
            request.release(),
            0,   // nodeIndex
            NTabletPipe::TClientConfig(),
            TActorId(),   // clientId
            42);          // cookie

        TAutoPtr<IEventHandle> handle;
        const auto* response =
            runtime.GrabEdgeEvent<TNbs1Volume::TEvWaitReadyResponse>(handle);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(42u, handle->Cookie);
        UNIT_ASSERT_C(
            !HasError(response->GetError()),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldKeepPartitionTabletIdAcrossRestart)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);
        BootVolumeTablet(runtime);

        const TActorId volumeActorId = ResolveTablet(runtime, VolumeTabletId);
        const TVolumeActor& volumeActor = FindVolume(runtime, volumeActorId);
        UNIT_ASSERT_VALUES_EQUAL(
            0u,
            TVolumeActorTestAccessor::PartitionTabletId(volumeActor));

        constexpr ui64 StoredPartitionTabletId = 1001;
        auto request =
            std::make_unique<NKikimr::TEvBlockStore::TEvUpdateVolumeConfig>();
        request->Record.SetTxId(7);
        auto* partition = request->Record.AddPartitions();
        partition->SetPartitionId(0);
        partition->SetTabletId(StoredPartitionTabletId);

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.SendToPipe(
            VolumeTabletId,
            edge,
            request.release(),
            0,   // nodeIndex
            NTabletPipe::TClientConfig(),
            TActorId(),   // clientId
            0);           // cookie

        // The condition only reads the actor. FindActor takes the runtime
        // lock, which DispatchEvents already holds.
        TDispatchOptions stored;
        stored.CustomFinalCondition = [&]()
        {
            return TVolumeActorTestAccessor::PartitionTabletId(volumeActor) ==
                   StoredPartitionTabletId;
        };
        UNIT_ASSERT(runtime.DispatchEvents(stored));

        RebootTablet(runtime, VolumeTabletId, edge);

        const TActorId restartedId = ResolveTablet(runtime, VolumeTabletId);
        const TVolumeActor& restarted = FindVolume(runtime, restartedId);
        TDispatchOptions loaded;
        loaded.CustomFinalCondition = [&]()
        {
            return TVolumeActorTestAccessor::PartitionTabletId(restarted) ==
                   StoredPartitionTabletId;
        };
        UNIT_ASSERT(runtime.DispatchEvents(loaded));
        UNIT_ASSERT_VALUES_EQUAL(
            StoredPartitionTabletId,
            TVolumeActorTestAccessor::PartitionTabletId(restarted));
    }
}

}   // namespace NYdb::NBS::NStorage
