#include "volume.h"

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

namespace {

////////////////////////////////////////////////////////////////////////////////

using TNbs1Service = NNbs1CompatApi::NBlockStore::TEvService;
using TNbs1Volume = NNbs1CompatApi::NBlockStore::TEvVolume;

const ui64 VolumeTabletId = MakeTabletID(0, 0, 1);

void BootVolumeTablet(TTestBasicRuntime& runtime)
{
    CreateTestBootstrapper(
        runtime,
        CreateTestTabletInfo(VolumeTabletId, TTabletTypes::Unknown),
        &CreateVolumeTablet);

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
    runtime.DispatchEvents(options);
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
}

}   // namespace NYdb::NBS::NStorage
