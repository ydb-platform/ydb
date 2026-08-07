#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_actor.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDbsControllerTest)
{
    Y_UNIT_TEST(ShouldBoot)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = MakeTabletID(0, 0, 1);

        CreateTestBootstrapper(
            runtime,
            CreateTestTabletInfo(tabletId, TTabletTypes::DbsController),
            [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
            { return new TDbsControllerActor(tablet, info); });

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(ShouldHandleUpdateDDiskMapRequest)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = MakeTabletID(0, 0, 1);

        {
            CreateTestBootstrapper(
                runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::DbsController),
                [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
                { return new TDbsControllerActor(tablet, info); });

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
            runtime.DispatchEvents(options);
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);

            const TActorId& edge = runtime.AllocateEdgeActor();

            runtime.SendToPipe(tabletId, edge, request.release());

            auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldHandleGetNodesForPartitionRequest)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = MakeTabletID(0, 0, 1);

        {
            CreateTestBootstrapper(
                runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::DbsController),
                [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
                { return new TDbsControllerActor(tablet, info); });

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
            runtime.DispatchEvents(options);
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvGetNodesForPartitionRequest>();
            request->Record.SetPartitionTabletId(1);

            const TActorId& edge = runtime.AllocateEdgeActor();

            runtime.SendToPipe(tabletId, edge, request.release());

            auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvGetNodesForPartitionResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldHandleGetPartitionsForNodeRequest)
    {
        TTestBasicRuntime runtime;
        SetupTabletServices(runtime);

        const ui64 tabletId = MakeTabletID(0, 0, 1);

        {
            CreateTestBootstrapper(
                runtime,
                CreateTestTabletInfo(tabletId, TTabletTypes::DbsController),
                [](const TActorId& tablet, TTabletStorageInfo* info) -> IActor*
                { return new TDbsControllerActor(tablet, info); });

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvTablet::EvBoot, 1);
            runtime.DispatchEvents(options);
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest>();
            request->Record.SetNodeId(1);

            const TActorId& edge = runtime.AllocateEdgeActor();

            runtime.SendToPipe(tabletId, edge, request.release());

            auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
