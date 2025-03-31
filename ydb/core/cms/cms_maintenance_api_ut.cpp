#include "cms_ut_common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCmsTest {

using namespace Ydb::Maintenance;

Y_UNIT_TEST_SUITE(TMaintenanceApiTest) {
    Y_UNIT_TEST(ManyActionGroupsWithSingleAction) {
        TCmsTestEnv env(8);

        auto response = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a1 = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(1).action_states().size(), 1);
        const auto &a2 = response.action_group_states(1).action_states(0);

        bool hasPerformedAction = a1.status() == ActionState::ACTION_STATUS_PERFORMED
            || a2.status() == ActionState::ACTION_STATUS_PERFORMED;
        bool hasPendingAction = a1.status() == ActionState::ACTION_STATUS_PENDING
            || a2.status() == ActionState::ACTION_STATUS_PENDING;
        UNIT_ASSERT(hasPerformedAction && hasPendingAction);
    }

    Y_UNIT_TEST(SingleCompositeActionGroup) {
        TCmsTestEnv env(16);

        // lock a node to prevent task-2 from taking a lock on a node from the same storage group
        env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );
        
        auto response = env.CheckMaintenanceTaskCreate("task-2", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10)),
                MakeLockAction(env.GetNodeId(9), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 2);
        const auto &a1 = response.action_group_states(0).action_states(0);
        const auto &a2 = response.action_group_states(0).action_states(1);

        bool allActionsPending = a1.status() == ActionState::ACTION_STATUS_PENDING
            && a2.status() == ActionState::ACTION_STATUS_PENDING;
        UNIT_ASSERT(allActionsPending);
    }

    Y_UNIT_TEST(CompositeActionGroupSameStorageGroup) {
        TCmsTestEnv env(8);

        env.CheckMaintenanceTaskCreate("task-2", Ydb::StatusIds::BAD_REQUEST,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10)),
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );
    }

    Y_UNIT_TEST(ActionReason) {
        TCmsTestEnv env(8);

        auto response = env.CheckMaintenanceTaskCreate("task-1", Ydb::StatusIds::SUCCESS,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            ),
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(1), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a1 = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a1.status(), ActionState::ACTION_STATUS_PERFORMED);
        UNIT_ASSERT_VALUES_EQUAL(a1.reason(), ActionState::ACTION_REASON_OK);
        UNIT_ASSERT(a1.reason_details().empty());

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(1).action_states().size(), 1);
        const auto &a2 = response.action_group_states(1).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a2.status(), ActionState::ACTION_STATUS_PENDING);
        UNIT_ASSERT_VALUES_EQUAL(a2.reason(), ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS);
        UNIT_ASSERT(a2.reason_details().Contains("too many unavailable vdisks"));
    }

    Y_UNIT_TEST(SimplifiedMirror3DC) {
        TTestEnvOpts options(3);
        options.UseMirror3dcErasure = true;
        options.DataCenterCount = 3;
        TCmsTestEnv env(options);

        auto response = env.CheckMaintenanceTaskCreate(
            "task-1",
            Ydb::StatusIds::SUCCESS,
            Ydb::Maintenance::AVAILABILITY_MODE_WEAK,
            MakeActionGroup(
                MakeLockAction(env.GetNodeId(0), TDuration::Minutes(10))
            )
        );

        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.action_group_states(0).action_states().size(), 1);
        const auto &a = response.action_group_states(0).action_states(0);
        UNIT_ASSERT_VALUES_EQUAL(a.status(), ActionState::ACTION_STATUS_PERFORMED);
    }

    Y_UNIT_TEST(RequestReplaceDevicePDisk) {
        std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)> serializeIdFn = [](NCms::TPDiskID& pdiskId, Ydb::Maintenance::ActionScope_PDisk* pdisk) {
            auto* id = pdisk->mutable_pdisk_id();
            id->set_node_id(pdiskId.NodeId);
            id->set_pdisk_id(pdiskId.DiskId);
        };

        std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)> deserializeIdFn = [](const Ydb::Maintenance::ActionScope_PDisk& pdisk, NCms::TPDiskID& pdiskId) {
            auto& id = pdisk.Getpdisk_id();
            pdiskId.NodeId = id.node_id();
            pdiskId.DiskId = id.pdisk_id();
        };

        std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)> serializeLocationFn = [](NCms::TPDiskID& pdiskId, Ydb::Maintenance::ActionScope_PDisk* pdisk) {
            auto* location = pdisk->mutable_pdisk_location();
            location->set_host("::1"); // All nodes live on this host.
            location->set_path("/" + std::to_string(pdiskId.NodeId) + "/pdisk-" + std::to_string(pdiskId.DiskId) + ".data");
        };

        std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)> deserializeLocationFn = [](const Ydb::Maintenance::ActionScope_PDisk& pdisk, NCms::TPDiskID& pdiskId) {
            auto& location = pdisk.Getpdisk_location();
            sscanf(location.path().c_str(), "/%d/pdisk-%d.data", &pdiskId.NodeId, &pdiskId.DiskId);
        };

        std::vector<std::function<void(NCms::TPDiskID&, Ydb::Maintenance::ActionScope_PDisk*)>> serializeFns = {serializeIdFn, serializeLocationFn};

        std::vector<std::function<void(const Ydb::Maintenance::ActionScope_PDisk&, NCms::TPDiskID&)>> deserializeFns = {deserializeIdFn, deserializeLocationFn};

        for (size_t i = 0; i < serializeFns.size(); i++) {
            auto serializeFn = serializeFns[i];
            auto deserializeFn = deserializeFns[i];

            TTestEnvOpts envOpts(8);
            envOpts.WithSentinel();

            TCmsTestEnv env(envOpts);

            auto req = std::make_unique<NKikimr::NCms::TEvCms::TEvCreateMaintenanceTaskRequest>();

            Ydb::Maintenance::CreateMaintenanceTaskRequest* rec = req->Record.MutableRequest();

            req->Record.SetUserSID("user");

            auto* options = rec->mutable_task_options();
            options->set_availability_mode(::Ydb::Maintenance::AVAILABILITY_MODE_STRONG);

            auto* actionGroup = rec->add_action_groups();
            auto* action = actionGroup->Addactions();
            auto* lockAction = action->mutable_lock_action();

            auto* scope = lockAction->mutable_scope();

            auto* pdisk = scope->mutable_pdisk();
            NCms::TPDiskID disk = env.PDiskId(0);
            serializeFn(disk, pdisk);
            lockAction->mutable_duration()->set_seconds(60);

            env.SendToCms(req.release());

            auto response = env.GrabEdgeEvent<NCms::TEvCms::TEvMaintenanceTaskResponse>();

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), Ydb::StatusIds_StatusCode_SUCCESS);

            {
                auto& result = response->Record.GetResult();
                UNIT_ASSERT_VALUES_EQUAL(result.action_group_states().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(result.action_group_states(0).action_states().size(), 1);
                const auto &actionState = result.action_group_states(0).action_states(0);

                UNIT_ASSERT(actionState.has_action());
                const auto& action = actionState.action();
                UNIT_ASSERT(action.has_lock_action());
                const auto& lockAction = action.lock_action();

                UNIT_ASSERT(lockAction.has_scope());
                const auto& scope = lockAction.scope();
                UNIT_ASSERT(scope.has_pdisk());
                const auto& pdisk = scope.pdisk();

                NCms::TPDiskID resultDisk;
                deserializeFn(pdisk, resultDisk);
                UNIT_ASSERT_VALUES_EQUAL(resultDisk, disk);
            }

            env.CheckRequest("user", "user-r-1", false, NKikimrCms::TStatus::TStatus::WRONG_REQUEST);
        }
    }
}

} // namespace NKikimr::NCmsTest 
