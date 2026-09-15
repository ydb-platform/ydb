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

    Y_UNIT_TEST(ShouldQueryOverStoredData)
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

        const TActorId& edge = runtime.AllocateEdgeActor();

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);
            auto* ddisks = request->Record.MutablePartitionDDisks();
            auto* dbgDDisks = ddisks->AddDirectBlockGroupsDDisks();
            auto* ids = dbgDDisks->AddDDiskIds();
            {
                auto* ddiskId = ids->MutableDDisk();
                ddiskId->SetNodeId(1);
                ddiskId->SetPDiskId(1);
                ddiskId->SetDDiskSlotId(1);
            }
            {
                auto* pBufferId = ids->MutablePersistentBuffer();
                pBufferId->SetNodeId(1);
                pBufferId->SetPDiskId(1);
                pBufferId->SetDDiskSlotId(2);
            }

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest>();
            request->Record.SetNodeId(1);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.PartitionsSize());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetPartitions(0));
        }
    }

    Y_UNIT_TEST(ShouldClearTabletData)
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

        const TActorId& edge = runtime.AllocateEdgeActor();

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);
            auto* ddisks = request->Record.MutablePartitionDDisks();
            auto* dbgDDisks = ddisks->AddDirectBlockGroupsDDisks();
            auto* ids = dbgDDisks->AddDDiskIds();
            {
                auto* ddiskId = ids->MutableDDisk();
                ddiskId->SetNodeId(1);
                ddiskId->SetPDiskId(1);
                ddiskId->SetDDiskSlotId(1);
            }
            {
                auto* pBufferId = ids->MutablePersistentBuffer();
                pBufferId->SetNodeId(1);
                pBufferId->SetPDiskId(1);
                pBufferId->SetDDiskSlotId(2);
            }

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapRequest>();
            request->Record.SetTabletId(1);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest>();
            request->Record.SetNodeId(1);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvGetPartitionsForNodeResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.PartitionsSize());
        }
    }

    Y_UNIT_TEST(ShouldGivePermissionForOneNodeOnCompleteDBG)
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

        const TActorId& edge = runtime.AllocateEdgeActor();

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);
            auto* ddisks = request->Record.MutablePartitionDDisks();
            auto* dbgDDisks = ddisks->AddDirectBlockGroupsDDisks();
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(1);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(1);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(2);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(2);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(3);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(3);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(4);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(4);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(5);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(5);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest>();
            request->Record.AddNodeIds(1);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response =
                runtime
                    .GrabEdgeEvent<TEvDbsControllerPrivate::
                                       TEvNodeMaintenancePermissionResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT(NProto::ALLOW == response->Record.GetDecision());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.BlockingPartitionIdsSize());
        }
    }

    Y_UNIT_TEST(ShouldNotGivePermissionForTwoNodesOnCompleteDBG)
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

        const TActorId& edge = runtime.AllocateEdgeActor();

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);
            auto* ddisks = request->Record.MutablePartitionDDisks();
            auto* dbgDDisks = ddisks->AddDirectBlockGroupsDDisks();
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(1);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(1);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(2);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(2);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(3);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(3);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(4);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(4);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(5);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(5);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest>();
            request->Record.AddNodeIds(1);
            request->Record.AddNodeIds(2);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response =
                runtime
                    .GrabEdgeEvent<TEvDbsControllerPrivate::
                                       TEvNodeMaintenancePermissionResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT(NProto::DENY == response->Record.GetDecision());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.BlockingPartitionIdsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetBlockingPartitionIds(0));
        }
    }

    Y_UNIT_TEST(ShouldNotGivePermissionForOneNodeOnIncompleteDBG)
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

        const TActorId& edge = runtime.AllocateEdgeActor();

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest>();
            request->Record.SetTabletId(1);
            auto* ddisks = request->Record.MutablePartitionDDisks();
            auto* dbgDDisks = ddisks->AddDirectBlockGroupsDDisks();
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(1);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(1);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(2);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(2);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(3);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(3);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }
            {
                auto* ids = dbgDDisks->AddDDiskIds();
                {
                    auto* ddiskId = ids->MutableDDisk();
                    ddiskId->SetNodeId(4);
                    ddiskId->SetPDiskId(1);
                    ddiskId->SetDDiskSlotId(1);
                }
                {
                    auto* pBufferId = ids->MutablePersistentBuffer();
                    pBufferId->SetNodeId(4);
                    pBufferId->SetPDiskId(1);
                    pBufferId->SetDDiskSlotId(2);
                }
            }

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response = runtime.GrabEdgeEvent<
                TEvDbsControllerPrivate::TEvUpdateDDiskMapResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
        }

        {
            auto request = std::make_unique<
                TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest>();
            request->Record.AddNodeIds(1);

            runtime.SendToPipe(tabletId, edge, request.release());

            const auto response =
                runtime
                    .GrabEdgeEvent<TEvDbsControllerPrivate::
                                       TEvNodeMaintenancePermissionResponse>();

            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT(NProto::DENY == response->Record.GetDecision());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.BlockingPartitionIdsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetBlockingPartitionIds(0));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
